package loghub;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

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
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;

public class TestVarFormatter {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
        Configurator.setLevel("loghub.VarFormatter", Level.DEBUG);
    }

    private void checkFormat(Object value, String format) {
        boolean isWithShortMonth = "%tB".equalsIgnoreCase(format)
                || "%th".equalsIgnoreCase(format)
                || "%tc".equals(format);
        boolean isIslamicChronology;
        if (value instanceof ChronoZonedDateTime) {
            ChronoZonedDateTime<?> czdt = (ChronoZonedDateTime<?>) value;
            isIslamicChronology = "islamic-umalqura".equals(czdt.getChronology().getCalendarType());
        } else {
            isIslamicChronology = false;
        }
        if (isIslamicChronology && isWithShortMonth) {
            // this chronology is not well-supported in String.format, for all versions up to 15
            return;
        }
        for (Locale l : Locale.getAvailableLocales()) {
            testWithLocale(l, value, format);
        }
    }

    private void testWithLocale(Locale l, Object value, String format) {
        VarFormatter vf = new VarFormatter("${" + format + "}", l);
        String printf;
        if (value instanceof Instant) {
            Instant i = (Instant) value;
            TemporalAccessor ta = VarFormatter.resolveWithEra(l, ZonedDateTime.ofInstant(i, ZoneId.systemDefault()));
            printf = String.format(l, format, ta);
        } else {
            printf = String.format(l, format, value);
        }
        try {
            String formatter = vf.format(value);
            Assert.assertEquals("mismatch for " + format + " at locale " + l.toLanguageTag() + " with " + value.getClass().getSimpleName(), printf, formatter);
        } catch (IllegalArgumentException e) {
            Assert.fail("mismatch for " + format + " at locale " + l.toLanguageTag() + " with " + value.getClass().getSimpleName() + ": " + Helpers.resolveThrowableException(e));
        }
    }

    @Test
    public void test1() {
        checkFormat(65535, "%#X");
        checkFormat(65535, "%#x");
    }

    @Test
    public void test2() {
        checkFormat(65535, "%#o");
        // unknown to format checkFormat(65535, "%#O");
    }

    @Test
    public void test3() {
        checkFormat(65535, "%-10d");
        checkFormat(65535, "%10d");
        checkFormat(65535, "%d");
        checkFormat(65535, "%010d");
        checkFormat(-65535, "%(d");
    }

    @Test
    public void test4() {
        checkFormat(Math.PI, "%f");
        checkFormat(Math.PI, "%10.2f");
        checkFormat(Math.PI, "%+10.2f");
        checkFormat(Math.PI, "% 10.2f");
        checkFormat(Math.PI, "%-10.2f");
        checkFormat(Math.PI, "%010.2f");
        // %f add an exact number of floating digit, even if they are 0
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
        AssertionError ex = Assert.assertThrows(AssertionError.class, () -> testWithLocale(Locale.ENGLISH, -1.0, "%010.2f"));
        Assert.assertEquals("mismatch for %010.2f at locale en with Double expected:<-00000[]1.00> but was:<-00000[0]1.00>", ex.getMessage());

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

    @Test
    public void testWeek() {
        // The values to test was generated using:
        // // LANG=C TZ=UTC date -d 'YYYY-01-dd 00:00:00-00:00' '+%Y.%m.%d=%s %V'
        long[] times = {
                1230768000, // 2009.01.01=1230768000 01
                1199145600, // 2008.01.01=1199145600 01
                1167609600, // 2007.01.01=1167609600 01
                1136073600, // 2006.01.01=1136073600 52
                1104537600, // 2005.01.01=1104537600 53
                1072915200, // 2004.01.01=1072915200 01
                1041379200, // 2003.01.01=1041379200 01
                1041465600, // 2003.01.02=1041465600 01
                1041552000, // 2003.01.03=1041552000 01
                1041638400, // 2003.01.04=1041638400 01
                1041724800, // 2003.01.05=1041724800 01
                1041811200, // 2003.01.06=1041811200 02
                1041897600, // 2003.01.07=1041897600 02
                1041984000, // 2003.01.08=1041984000 02
        };
        String[] weekNum = {
                "01",
                "01",
                "01",
                "52",
                "53",
                "01",
                "01",
                "01",
                "01",
                "01",
                "01",
                "02",
                "02",
                "02",
        };
        VarFormatter formatter = new VarFormatter("${%t<UTC>V}", Locale.US);
        for (int i = 0; i < times.length; i++) {
            String formatted = formatter.format(Instant.ofEpochSecond(times[i]));
            Assert.assertEquals(weekNum[i], formatted);
        }
    }

    private <T> T readJson(Object toSerialize) throws JsonProcessingException {
        ObjectReader reader = JacksonBuilder.get(JsonMapper.class).getReader();
        VarFormatter formatter = new VarFormatter("${%j}");
        String serialized = formatter.format(toSerialize);
        return reader.readValue(serialized);
    }

    @Test
    public void testJson() throws JsonProcessingException {
        Map<String, Object> mapping = Map.of("a", 1, "b", List.of(2, 3, 4), "c", Instant.ofEpochSecond(0));
        Map<String, Object> mapped = readJson(mapping);
        Assert.assertEquals(mapping.get("a"), mapped.get("a"));
        Assert.assertEquals(mapping.get("b"), mapped.get("b"));
        Assert.assertEquals("1970-01-01T00:00:00Z", mapped.get("c"));

        Event ev = factory.newEvent();
        ev.setTimestamp(Instant.ofEpochSecond(0));
        Map<String, Object> eventMapped = readJson(ev);
        @SuppressWarnings("unchecked")
        Map<String, Object> eventMap = (Map<String, Object>) eventMapped.get("loghub.Event");
        Assert.assertEquals("1970-01-01T00:00:00Z", eventMap.get("@timestamp"));
        Assert.assertEquals(Collections.emptyMap(), eventMap.get("@fields"));
        Assert.assertEquals(Collections.emptyMap(), eventMap.get("@METAS"));

        Assert.assertEquals("loghub", readJson("loghub"));
    }

    @Test
    public void testDateFormatDate() {
        checkDate(new Date());
    }

    @Test
    public void testDateFormatZonedDateTime() {
        checkDate(ZonedDateTime.now());
    }

    @Test
    public void testDateFormatInstant() {
        checkDate(Instant.now());
    }

    @Test
    public void testDateFormatCalendar() {
        checkDate(Calendar.getInstance());
    }

    @Test
    public void testDateFormatHijrahChronology() {
        ChronoZonedDateTime<HijrahDate> h = HijrahDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault());
        checkDate(h);
    }

    @Test
    public void testDateFormatJapaneseChronology() {
        ChronoZonedDateTime<JapaneseDate> h = JapaneseDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault());
        checkDate(h);
    }

    @Test
    public void testDateFormatMinguoChronology() {
        ChronoZonedDateTime<MinguoDate> h = MinguoDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault());
        checkDate(h);
    }

    @Test
    public void testDateFormatThaiBuddhistChronology() {
        ChronoZonedDateTime<ThaiBuddhistDate> h = ThaiBuddhistDate.now().atTime(LocalTime.now()).atZone(ZoneId.systemDefault());
        checkDate(h);
    }

    @Test
    public void testDefaultTz() {
        ZonedDateTime now = ZonedDateTime.now();
        VarFormatter vf = new VarFormatter("${%t<>Z}", Locale.US);
        String formatter = vf.format(now);
        Assert.assertEquals("Mismatch for time zone parsing", String.format(Locale.US, "%tZ", ZonedDateTime.now()), formatter);
    }

    @Test
    public void testPreservedTz() {
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
        Assert.assertEquals("Mismatch for time zone parsing", String.format(Locale.US, "%tZ", ZonedDateTime.now(other)), formatter);
    }

    @Test
    public void testTimeZoneLocale() {
        ZonedDateTime now = ZonedDateTime.now();
        Locale[] locales = Locale.getAvailableLocales();
        for (String ziName : ZoneId.getAvailableZoneIds()) {
            ZoneId zi = ZoneId.of(ziName);
            ZonedDateTime znow = now.withZoneSameInstant(zi);
            String formatter1 = String.format("${%%t<%s>z}", ziName);
            String formatter2 = String.format("${%%t<%s>Z}", ziName);
            for (Locale l : locales) {
                String printfz = String.format(l, "%tz", znow);
                String printfZ = String.format(l, "%tZ", znow);
                VarFormatter vfz = new VarFormatter(formatter1, l);
                VarFormatter vfZ = new VarFormatter(formatter2, l);
                Assert.assertEquals(printfz, vfz.format(znow));
                Assert.assertEquals(printfZ, vfZ.format(znow));
            }
        }
    }

    @Test
    public void testEscape() {
        Map<String, Object> values = Collections.singletonMap("var", 1);
        VarFormatter vf = new VarFormatter("a${}b{}c{.}d");
        String formatter = vf.format(values);
        Assert.assertEquals("mismatch for string escape", "a${}b{}c{.}d", formatter);

        vf = new VarFormatter("a'c");
        formatter = vf.format(values);
        Assert.assertEquals("mismatch for string escape", "a'c", formatter);
    }

    @Test
    public void testMany() {
        Map<String, Object> values = new HashMap<>() {{
            put("a", 2);
            put("b", 1);
        }};
        VarFormatter vf = new VarFormatter("'${a}${}${b}'");
        Assert.assertEquals("mismatch for complex pattern", "'2${}1'", vf.format(values));
    }

    @Test
    public void formatSimple() {
        VarFormatter vf = new VarFormatter("${%d} ${%04d}");
        Assert.assertEquals("mismatch for complex pattern", "123 0123", vf.format(123));
    }

    @Test
    public void formatPath() {
        VarFormatter vf = new VarFormatter("${a.b}", Locale.ENGLISH);
        Map<String, Map<String, Object>> obj = Collections.singletonMap("a", Collections.singletonMap("b", "c"));
        String formatted = vf.format(obj);
        Assert.assertEquals("c", formatted);
    }

    @Test
    public void formatArgs() {
        VarFormatter vf = new VarFormatter("${#1%s} ${#1%s} ${#3%s}", Locale.ENGLISH);
        String formatted = vf.argsFormat("1", "2", "3");
        Assert.assertEquals("1 1 3", formatted);
    }

    @Test
    public void formatArray() {
        VarFormatter vf = new VarFormatter("${#1%s} ${#1%s} ${#3%s}", Locale.ENGLISH);
        List<String> obj = Arrays.asList("1", "2", "3");
        String formatted = vf.format(obj);
        Assert.assertEquals("1 1 3", formatted);
    }

    @Test
    public void formatListString() {
        VarFormatter vf = new VarFormatter("${%s}", Locale.ENGLISH);
        List<String> obj = Arrays.asList("1", "2", "3");
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatArrayString() {
        VarFormatter vf = new VarFormatter("${%s}", Locale.ENGLISH);
        String[] obj = new String[] {"1", "2", "3"};
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatDeepArrayString() {
        VarFormatter vf = new VarFormatter("${%s}", Locale.ENGLISH);
        Object[] obj = new Object[] {"1", "2", new Object[] {"3"}};
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, [3]]", formatted);
    }

    @Test
    public void formatArrayIntString() {
        VarFormatter vf = new VarFormatter("${%s}", Locale.ENGLISH);
        int[] obj = new int[] {1, 2, 3};
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatArrayMapString() {
        VarFormatter vf = new VarFormatter("${a%s}", Locale.ENGLISH);
        Map<String, Object> values = Collections.singletonMap("a", new int[] {1, 2, 3});
        String formatted = vf.format(values);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatArrayMapDepthString() {
        VarFormatter vf = new VarFormatter("${a.b%s}", Locale.ENGLISH);
        Map<String, Object> values = Collections.singletonMap("a", Collections.singletonMap("b", new int[] {1, 2, 3}));
        String formatted = vf.format(values);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatNewLine() {
        VarFormatter vf = new VarFormatter("${#1%s}\n${#1%s}\n${#3%s}", Locale.ENGLISH);
        List<String> obj = Arrays.asList("1", "2", "3");
        String formatted = vf.format(obj);
        Assert.assertEquals("1\n1\n3", formatted);
    }

    @Test
    public void formatAllImplicit() {
        VarFormatter vf = new VarFormatter("${%s}", Locale.ENGLISH);
        List<String> obj = Arrays.asList("1", "2", "3");
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test
    public void formatAllExplicit() {
        VarFormatter vf = new VarFormatter("${.%s}", Locale.ENGLISH);
        List<String> obj = List.of("1", "2", "3");
        String formatted = vf.format(obj);
        Assert.assertEquals("[1, 2, 3]", formatted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError1() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b}");
        vf.format(values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError2() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b.c}");
        vf.format(values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError3() {
        Map<String, Object> values = Collections.singletonMap("a", 1);
        VarFormatter vf = new VarFormatter("${b.c}");
        vf.format(values);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError4() {
        VarFormatter vf = new VarFormatter("${#2%s}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        String formatted = vf.format(obj);
        Assert.assertEquals("1", formatted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError5() {
        VarFormatter vf = new VarFormatter("${a} ${#2}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        String formatted = vf.format(obj);
        Assert.assertEquals("1", formatted);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError6() {
        VarFormatter vf = new VarFormatter("${a}", Locale.ENGLISH);
        List<Object> obj = Collections.singletonList(1);
        vf.format(obj);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError7() {
        VarFormatter vf = new VarFormatter("${#1}", Locale.ENGLISH);
        Map<String, Object> obj = Collections.singletonMap("a", 1);
        vf.format(obj);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError8() {
        VarFormatter vf = new VarFormatter("${var%z}", Locale.ENGLISH);
        Map<String, Object> obj = Collections.singletonMap("a", 1);
        vf.format(obj);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testError9() {
        VarFormatter vf = new VarFormatter("${var%zd}", Locale.ENGLISH);
        Map<String, Object> obj = Collections.singletonMap("a", 1);
        vf.format(obj);
    }

}
