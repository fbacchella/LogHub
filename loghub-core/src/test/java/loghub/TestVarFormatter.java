package loghub;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;
import java.time.chrono.HijrahDate;
import java.time.chrono.JapaneseDate;
import java.time.chrono.MinguoDate;
import java.time.chrono.ThaiBuddhistDate;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;

class TestVarFormatter {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
        Configurator.setLevel("loghub.VarFormatter", Level.DEBUG);
    }

    private void checkFormat(Object value, String format) {
        boolean isWithShortMonth = "%tB".equalsIgnoreCase(format)
                || "%th".equalsIgnoreCase(format)
                || "%tc".equals(format);
        boolean isIslamicChronology;
        if (value instanceof ChronoZonedDateTime<?> czdt) {
            isIslamicChronology = "islamic-umalqura".equals(czdt.getChronology().getCalendarType());
        } else {
            isIslamicChronology = false;
        }
        if (isIslamicChronology && isWithShortMonth) {
            // this chronology is not well-supported in String.format, for all versions up to 15
            return;
        }
        Arrays.stream(Locale.getAvailableLocales()).parallel().forEach(l -> {
            testWithLocale(l, value, format);
        });
    }

    private void testWithLocale(Locale l, Object value, String format) {
        VarFormatter vf = new VarFormatter("${" + format + "}", l);
        String printf;
        if (value instanceof Instant i) {
            TemporalAccessor ta = VarFormatter.resolveWithEra(l, ZonedDateTime.ofInstant(i, ZoneId.systemDefault()));
            printf = String.format(l, format, ta);
        } else {
            printf = String.format(l, format, value);
        }
        try {
            String formatter = vf.format(value);
            String className = value == null ? "null" : value.getClass().getSimpleName();
            Assertions.assertEquals(printf, formatter, () -> "mismatch for " + format + " at locale " + l.toLanguageTag() + " with " + className);
        } catch (IllegalArgumentException e) {
            String className = value == null ? "null" : value.getClass().getSimpleName();
            Assertions.fail("mismatch for " + format + " at locale " + l.toLanguageTag() + " with " + className + ": " + Helpers.resolveThrowableException(e));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"%#X", "%#x", "%#o", "%-10d", "%10d", "%d", "%010d"})
    void testNumbers(String format) {
        checkFormat(65535, format);
    }

    @Test
    void testBoolean() {
        checkFormat(true, "%b");
        checkFormat(false, "%b");
        checkFormat(true, "%B");
        checkFormat(false, "%B");
        checkFormat(null, "%b");
        checkFormat(new Object(), "%b");
        Assertions.assertEquals("false", new VarFormatter("${%b}").format(NullOrMissingValue.NULL));
        Assertions.assertEquals("FALSE", new VarFormatter("${%B}").format(NullOrMissingValue.NULL));
    }

    @Test
    void testHash() {
        checkFormat(this, "%h");
        checkFormat(this, "%H");
        Assertions.assertEquals("null", new VarFormatter("${%h}").format(null));
        Assertions.assertEquals("null", new VarFormatter("${%h}").format(NullOrMissingValue.NULL));
    }

    @Test
    void testNegativeNumber() {
        checkFormat(-65535, "%(d");
    }

    @Test
    void testFloat() {
        checkFormat(null, "%f");
        checkFormat(Math.PI, "%f");
        checkFormat(Math.PI, "%10.2f");
        checkFormat(Math.PI, "%+10.2f");
        checkFormat(Math.PI, "% 10.2f");
        checkFormat(Math.PI, "%-10.2f");
        checkFormat(Math.PI, "%010.2f");
        // %f add an exact number of floating digits, even if they are 0
        checkFormat(1.0, "%f");
        checkFormat(1.0, "%10.2f");
        checkFormat(1.0, "%+10.2f");
        checkFormat(1.0, "%-10.2f");
        checkFormat(1.0, "%010.2f");
        // - is a pain to check in many languages, just check for common english
        testWithLocale(Locale.ENGLISH, -1.0, "%f");
        testWithLocale(Locale.ENGLISH, -1.0, "%10.2f");
        testWithLocale(Locale.ENGLISH, -1.0, "%+10.2f");
        testWithLocale(Locale.ENGLISH, -1.0, "%-10.2f");
        // DecimalFormat and printf are unable to agree about maximum length for negative number padded with 0, too much work to fix
        AssertionError ex = Assertions.assertThrows(AssertionError.class, () -> testWithLocale(Locale.ENGLISH, -1.0, "%010.2f"));
        Assertions.assertEquals("mismatch for %010.2f at locale en with Double ==> expected: <-000001.00> but was: <-0000001.00>", ex.getMessage());
    }

    private void checkDate(Object date) {
        checkFormat(date, "%tH");
        checkFormat(date, "%tI");
        checkFormat(date, "%tk");
        checkFormat(date, "%tl");
        checkFormat(date, "%tM");
        checkFormat(date, "%tS");
        checkFormat(date, "%tL");
        checkFormat(date, "%tN");
        checkFormat(date, "%tp");
        checkFormat(date, "%tz");
        checkFormat(date, "%tZ");
        checkFormat(date, "%ts");
        checkFormat(date, "%tQ");
        checkFormat(date, "%tB");
        checkFormat(date, "%tb");
        checkFormat(date, "%th");
        checkFormat(date, "%tA");
        checkFormat(date, "%ta");
        checkFormat(date, "%tC");
        checkFormat(date, "%tY");
        checkFormat(date, "%ty");
        checkFormat(date, "%tj");
        checkFormat(date, "%tm");
        checkFormat(date, "%td");
        checkFormat(date, "%te");
        checkFormat(date, "%tR");
        checkFormat(date, "%tT");
        checkFormat(date, "%tr");
        checkFormat(date, "%tD");
        checkFormat(date, "%tF");
        checkFormat(date, "%tc");
        checkFormat(date, "%Ta");
        checkFormat(date, "%TA");
        checkFormat(date, "%Tp");
    }

    @ParameterizedTest
    @CsvSource({
            "1230768000, 01",
            "1199145600, 01",
            "1167609600, 01",
            "1136073600, 52",
            "1104537600, 53",
            "1072915200, 01",
            "1041379200, 01",
            "1041465600, 01",
            "1041552000, 01",
            "1041638400, 01",
            "1041724800, 01",
            "1041811200, 02",
            "1041897600, 02",
            "1041984000, 02"
    })
    void testWeek(long timestamp, String expectedWeek) {
        VarFormatter formatter = new VarFormatter("${%t<UTC>V}", Locale.US);
        String formatted = formatter.format(Instant.ofEpochSecond(timestamp));
        Assertions.assertEquals(expectedWeek, formatted);
    }

    private <T> T readJson(Object toSerialize) throws JsonProcessingException {
        ObjectReader reader = JacksonBuilder.get(JsonMapper.class).getReader();
        VarFormatter formatter = new VarFormatter("${%j}");
        String serialized = formatter.format(toSerialize);
        return reader.readValue(serialized);
    }

    @Test
    void testJson() throws JsonProcessingException {
        Map<String, Object> mapping = Map.of("a", 1, "b", List.of(2, 3, 4), "c", Instant.ofEpochSecond(0));
        Map<String, Object> mapped = readJson(mapping);
        Assertions.assertEquals(mapping.get("a"), mapped.get("a"));
        Assertions.assertEquals(mapping.get("b"), mapped.get("b"));
        Assertions.assertEquals("1970-01-01T00:00:00Z", mapped.get("c"));

        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochSecond(0));
        Map<String, Object> eventMapped = readJson(ev);
        @SuppressWarnings("unchecked")
        Map<String, Object> eventMap = (Map<String, Object>) eventMapped.get("loghub.Event");
        Assertions.assertEquals("1970-01-01T00:00:00Z", eventMap.get("@timestamp"));
        Assertions.assertEquals(Collections.emptyMap(), eventMap.get("@fields"));
        Assertions.assertEquals(Collections.emptyMap(), eventMap.get("@METAS"));

        Assertions.assertEquals("loghub", readJson("loghub"));
    }

    @ParameterizedTest
    @MethodSource("dateProvider")
    void testDateFormat(Object date) {
        checkDate(date);
    }

    static List<Object> dateProvider() {
        return Arrays.asList(
                new Date(),
                ZonedDateTime.now(),
                Instant.now(),
                Calendar.getInstance(),
                HijrahDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault()),
                JapaneseDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault()),
                MinguoDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault()),
                ThaiBuddhistDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault())
        );
    }

    @Test
    void testDefaultTz() {
        ZonedDateTime now = ZonedDateTime.now();
        VarFormatter vf = new VarFormatter("${%t<>Z}", Locale.US);
        String formatter = vf.format(now);
        Assertions.assertEquals(String.format(Locale.US, "%tZ", ZonedDateTime.now()), formatter, "Mismatch for time zone parsing");
    }

    @Test
    void testPreservedTz() {
        ZonedDateTime now = ZonedDateTime.now();
        ZoneId other = null;
        ZoneId system = ZoneId.systemDefault();
        for (String ziName : ZoneId.getAvailableZoneIds()) {
            ZoneId zi = ZoneId.of(ziName);
            if (zi != system) {
                other = zi;
                break;
            }
        }
        String formatting = String.format("${%%t<%s>Z}", other.getId());
        VarFormatter vf = new VarFormatter(formatting, Locale.US);
        String formatter = vf.format(now);
        Assertions.assertEquals(String.format(Locale.US, "%tZ", ZonedDateTime.now(other)), formatter, "Mismatch for time zone parsing");
    }

    private record FormatterSet(ZonedDateTime znow, String formatter1, String formatter2) {}
    private record LocalFormatterSet(FormatterSet fs, Locale l) {}

    @Test
    void testTimeZoneLocale() {
        ZonedDateTime now = ZonedDateTime.now();
        Locale[] locales = Locale.getAvailableLocales();
        ZoneId.getAvailableZoneIds()
              .stream()
              .map(ziName -> {
            ZoneId zi = ZoneId.of(ziName);
            ZonedDateTime znow = now.withZoneSameInstant(zi);
            String formatter1 = String.format("${%%t<%s>z}", ziName);
            String formatter2 = String.format("${%%t<%s>Z}", ziName);
            return new FormatterSet(znow, formatter1, formatter2);
        })
              .parallel()
              .flatMap(fs  -> Arrays.stream(locales).map(l -> new LocalFormatterSet(fs, l)))
              .forEach(lfs -> {
            String printfz = String.format(lfs.l, "%tz", lfs.fs.znow);
            String printfZ = String.format(lfs.l, "%tZ", lfs.fs.znow);
            VarFormatter vfz = new VarFormatter(lfs.fs.formatter1, lfs.l);
            VarFormatter vfZ = new VarFormatter(lfs.fs.formatter2, lfs.l);
            Assertions.assertEquals(printfz, vfz.format(lfs.fs.znow));
            Assertions.assertEquals(printfZ, vfZ.format(lfs.fs.znow));
        });
    }

    @Test
    void testEscape() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        VarFormatter vf = new VarFormatter("a${}b{}c{.}d");
        String formatter = vf.format(values);
        Assertions.assertEquals("a${}b{}c{.}d", formatter, "mismatch for string escape");

        vf = new VarFormatter("a'c");
        formatter = vf.format(values);
        Assertions.assertEquals("a'c", formatter, "mismatch for string escape");
    }

    @Test
    void testMany() {
        Map<String, Object> values = new HashMap<>() {{
            put("a", 2);
            put("b", 1);
        }};
        VarFormatter vf = new VarFormatter("'${a}${}${b}'");
        Assertions.assertEquals("'2${}1'", vf.format(values), "mismatch for complex pattern");
    }

    @Test
    void formatSimple() {
        VarFormatter vf = new VarFormatter("${%d} ${%04d}");
        Assertions.assertEquals("123 0123", vf.format(123), "mismatch for complex pattern");
    }

    @Test
    void formatPath() {
        VarFormatter vf = new VarFormatter("${a.b}", Locale.ENGLISH);
        Map<String, Map<String, Object>> obj = Collections.singletonMap("a", Collections.singletonMap("b", "c"));
        String formatted = vf.format(obj);
        Assertions.assertEquals("c", formatted);
    }

    @Test
    void formatEvent() {
        VarFormatter vf = new VarFormatter("${a.b%s} ${@timestamp%s}.${#meta%02d}", Locale.ENGLISH);
        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochSecond(0));
        ev.putAtPath(VariablePath.of("a", "b"), "c");
        ev.putMeta("meta", 1);
        String formatted = vf.format(ev);
        Assertions.assertEquals("c 1970-01-01T00:00:00Z.01", formatted);
    }

    @Test
    void formatArgs() {
        VarFormatter vf = new VarFormatter("${#1%s} ${#1%s} ${#3%s}", Locale.ENGLISH);
        String formatted = vf.argsFormat("1", "2", "3");
        Assertions.assertEquals("1 1 3", formatted);
    }

    @ParameterizedTest
    @MethodSource("formattedCollectionProvider")
    void testFormatCollections(String pattern, Object value, String expected) {
        VarFormatter vf = new VarFormatter(pattern, Locale.ENGLISH);
        String formatted = vf.format(value);
        Assertions.assertEquals(expected, formatted);
    }

    static List<Arguments> formattedCollectionProvider() {
        List<String> list = Arrays.asList("1", "2", "3");
        String[] array = new String[]{"1", "2", "3"};
        Object[] deepArray = new Object[]{"1", "2", new Object[]{"3"}};
        int[] intArray = new int[]{1, 2, 3};
        Map<String, Object> map = Collections.singletonMap("a", intArray);
        Map<String, Object> deepMap = Collections.singletonMap("a", Collections.singletonMap("b", intArray));

        return Arrays.asList(
                Arguments.of("${#1%s} ${#1%s} ${#3%s}", list, "1 1 3"),
                Arguments.of("${%s}", list, "[1, 2, 3]"),
                Arguments.of("${%s}", array, "[1, 2, 3]"),
                Arguments.of("${%s}", deepArray, "[1, 2, [3]]"),
                Arguments.of("${%s}", intArray, "[1, 2, 3]"),
                Arguments.of("${a%s}", map, "[1, 2, 3]"),
                Arguments.of("${a.b%s}", deepMap, "[1, 2, 3]"),
                Arguments.of("${#1%s}\n${#1%s}\n${#3%s}", list, "1\n1\n3"),
                Arguments.of("${%s}", list, "[1, 2, 3]"),
                Arguments.of("${.%s}", list, "[1, 2, 3]")
        );
    }

    @ParameterizedTest
    @MethodSource("errorProvider")
    void testErrors(String pattern, Object values) {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            VarFormatter vf = new VarFormatter(pattern, Locale.ENGLISH);
            vf.format(values);
        });
    }

    static List<Arguments> errorProvider() {
        Map<String, Object> map = Collections.singletonMap("a", 1);
        List<Object> list = Collections.singletonList(1);
        return Arrays.asList(
                Arguments.of("${b}", map),
                Arguments.of("${b.c}", map),
                Arguments.of("${#2%s}", list),
                Arguments.of("${a} ${#2}", list),
                Arguments.of("${a}", list),
                Arguments.of("${#1}", map),
                Arguments.of("${var%z}", map),
                Arguments.of("${var%zd}", map)
        );
    }

    @Test
    void testLiteral() {
        VarFormatter vf = new VarFormatter("${%%} ${%n}");
        Assertions.assertEquals("% " + System.lineSeparator(), vf.format(null));
        Assertions.assertEquals("% " + System.lineSeparator(), vf.format(NullOrMissingValue.NULL));
        Assertions.assertEquals("% " + System.lineSeparator(), vf.format(NullOrMissingValue.MISSING));
    }

    @ParameterizedTest
    @MethodSource("formatProvider")
    void testNull(String format) {
        if (!"%b".equals(format)) {
            Assertions.assertEquals("null", new VarFormatter("${" + format + "}").format(null));
        } else {
            Assertions.assertEquals("false", new VarFormatter("${" + format + "}").format(null));
        }
    }

    @ParameterizedTest
    @MethodSource("formatProvider")
    void testMissing(String format) {
        Assertions.assertThrows(IgnoredEventException.class, () -> new VarFormatter("${" + format + "}").format(NullOrMissingValue.MISSING));
    }

    static List<String> formatProvider() {
        return List.of("%h", "%s", "%d", "%tz", "%b", "%f", "%d", "%j");
    }

    @Test
    void testMissingKeyInEvent() {
        VarFormatter vf = new VarFormatter("${missing}", Locale.ENGLISH);
        Event ev = factory.newEvent();
        // Key "missing" is not in ev
        Assertions.assertThrows(IgnoredEventException.class, () -> {
            vf.format(ev);
        });
    }

}
