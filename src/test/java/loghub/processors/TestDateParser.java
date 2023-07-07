package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.axibase.date.PatternResolver;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestDateParser {

    private final EventsFactory factory = new EventsFactory();

    static private final Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void test1() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("ISO_DATE_TIME");
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        ZonedDateTime now = ZonedDateTime.now();
        event.put("field", PatternResolver.createNewFormatter("iso_nanos").print(now));
        parse.process(event);
        Instant parsedDate = (Instant) event.get("field");
        // ZonedDateTime is ms precision in J8 and µs in 11
        Assert.assertEquals("date not parsed", now.toInstant().toEpochMilli(), parsedDate.toEpochMilli());
    }

    @Test
    public void test1bis() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("yyyy-MM-ddTHH:mm:ss.SSSZ");
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        ZonedDateTime now = ZonedDateTime.now();
        event.put("field", PatternResolver.createNewFormatter("iso_nanos").print(now));
        parse.process(event);
        Instant parsedDate = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", now.toInstant(), parsedDate);
    }

    @Test
    public void test2() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSSSSXXX");
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1971-01-01T00:00:00.001001+01:00");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", "1970-12-31T23:00:00.001001Z", date.toString());
    }

    @Test
    public void test3() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("yyyy-MM-dd'T'HH:m:ss");
        builder.setTimezone(new Expression("Z"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1971-01-01T00:00:00");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", "1971-01-01T00:00:00Z", date.toString());
    }

    @Test
    public void test4() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "Tue, 3 Jun 2008 11:05:30 +0110");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", 1212486930000L, date.toEpochMilli());
    }

    @Test
    public void testTzLocale() throws ProcessorException {
        TimeZone defaultTz = TimeZone.getDefault();
        Locale defaultLocale = Locale.getDefault();

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Locale.setDefault(Locale.ENGLISH);

        ZonedDateTime now = ZonedDateTime.now();

        try {
            DateParser.Builder builder = DateParser.getBuilder();
            builder.setField(VariablePath.parse("field"));
            builder.setTimezone(new Expression(ZoneId.of("CET")));
            builder.setLocale(new Expression(Locale.FRANCE.toLanguageTag()));
            builder.setPattern("d MMM HH:mm:ss");
            DateParser parse = builder.build();
            Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

            Event event = factory.newEvent();
            event.put("field", "1 janv. 04:00:00");
            Assert.assertTrue(parse.process(event));
            Instant date = (Instant) event.get("field");
            ZonedDateTime zdt = ZonedDateTime.ofInstant(date, ZoneId.of("UTC"));
            // Hour 04 in CET is now parsed back as 03 in UTC
            Assert.assertEquals(3, zdt.get(ChronoField.HOUR_OF_DAY));
            // Year was set to current year
            Assert.assertEquals(now.get(ChronoField.YEAR), zdt.get(ChronoField.YEAR));
            // Month was parsed from janv (french) to january
            Assert.assertEquals(1, zdt.get(ChronoField.MONTH_OF_YEAR));
        } finally {
            TimeZone.setDefault(defaultTz);
            Locale.setDefault(defaultLocale);
        }
    }

    @Test
    public void testIncomplete() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("MMM dd HH:mm:ss");
        builder.setTimezone(new Expression("Z"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "Jul 26 16:40:22");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date, ZoneId.of("GMT"));
        int year = OffsetDateTime.now().get(ChronoField.YEAR);
        Assert.assertEquals("date not parsed", year, t.getLong(ChronoField.YEAR));
    }

    @Test
    public void testAgain() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSxx");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "2016-08-04T18:57:37.238+0000");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date, ZoneId.of("GMT"));
        Assert.assertEquals("date not parsed", 18, t.getLong(ChronoField.HOUR_OF_DAY));
    }

    @Test
    public void testFailedIso() throws ProcessorException {
        String fieldValue = "Jul 26 16:40:22";

        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("iso");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", fieldValue);
        Assert.assertFalse(parse.process(event));
        Assert.assertEquals(fieldValue, event.get("field"));
    }

    @Test
    public void testFailedIso2() throws ProcessorException {
        String fieldValue = "2023-06-08";

        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("ISO_INSTANT");
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", fieldValue);
        Assert.assertFalse(parse.process(event));
        Assert.assertEquals(fieldValue, event.get("field"));
    }

    @Test
    public void testFailedSyslog() throws ProcessorException {
        String fieldValue = "2016-08-04T18:57:37.238+0000";

        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("MMM dd HH:mm:ss");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", fieldValue);
        parse.process(event);
        Assert.assertEquals(fieldValue, event.get("field"));
    }

    @Test
    public void testAgain2() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("MMM dd HH:mm:ss.SSS");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "Jul 26 16:40:22.238");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date, ZoneId.of("GMT"));
        Assert.assertEquals("date not parsed", 14, t.getLong(ChronoField.HOUR_OF_DAY));
    }

    @Test
    public void testNumberInstantFloat() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("seconds");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Function<Instant, Number> tonumber = (i) -> 1.0 * (i.getEpochSecond()) + i.getNano()/1e9;
        resolve(parse, Instant.ofEpochSecond(155, 330000000), tonumber);
        resolve(parse, Instant.ofEpochSecond(155, 30), tonumber);
        resolve(parse, Instant.ofEpochSecond(-155, 330000000), tonumber);
        resolve(parse, Instant.ofEpochSecond(-155, -330000000), tonumber);
        resolve(parse, Instant.ofEpochSecond(-155, 30), tonumber);
    }

    @Test
    public void testNumberInstantInteger() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("seconds");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser processor = builder.build();
        Assert.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Function<Instant, Number> tonumber = Instant::getEpochSecond;
        resolve(processor, Instant.ofEpochSecond(155, 0), tonumber);
        resolve(processor, Instant.ofEpochSecond(-155, 0), tonumber);
    }

    @Test
    public void testNumberDate() throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("milliseconds");
        builder.setTimezone(new Expression("CET"));
        builder.setField(VariablePath.parse("field"));
        DateParser processor = builder.build();
        Assert.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Assert.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Function<Instant, Number> tonumber = Instant::toEpochMilli;
        resolve(processor, new Date(155).toInstant(), tonumber);
        resolve(processor, new Date(-155).toInstant(), tonumber);
    }

    private void resolve(DateParser processor, Instant instant,
                         Function<Instant, Number> tonumber) throws ProcessorException {
        Event event = factory.newEvent();
        event.put("field", tonumber.apply(instant));
        processor.process(event);
        Instant o = (Instant) event.get("field");
        Assert.assertEquals(instant, o);
    }

    @Test
    public void testBadPattern() {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern("failed");
        IllegalArgumentException ex = Assert.assertThrows(IllegalArgumentException.class, builder::build);
        Assert.assertEquals("Unknown pattern letter: f", ex.getMessage());
    }

    private void checkTZ(String tz, String tzFormat, int expectedHour) throws ProcessorException {
        String parseformat = "yyyy MMM dd HH:mm:ss.SSS " + tzFormat;
        String parsedDate = "2019 Sep 18 07:53:09.504 " + tz;

        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPatterns(new String[] {parseformat});
        builder.setTimezone(new Expression("America/Los_Angeles"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", parsedDate);
        Assert.assertTrue("failed to parse " + parsedDate + " with " + parseformat, parse.process(event));
        Instant date = (Instant) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date, ZoneId.of("GMT"));
        Assert.assertEquals("failed to parse " + parsedDate + " with " + parseformat, expectedHour, t.getLong(ChronoField.HOUR_OF_DAY));
    }

    @Test
    public void testTZFormats() throws ProcessorException {
        checkTZ("UTC", "zzz", 7);
        checkTZ("Z", "zzz", 7);
        checkTZ("GMT", "zzz", 7);
        checkTZ("CET", "zzz", 5);
        checkTZ("Europe/Paris", "VV", 5);
        checkTZ("+0200", "Z", 5);
        checkTZ("+02:00", "xxx", 5);
        checkTZ("", "", 14);
    }

    private void checkPattern(String patternName, String parseDate, Instant expected) throws ProcessorException {
        DateParser.Builder builder = DateParser.getBuilder();
        builder.setPattern(patternName);
        builder.setLocale(new Expression(Locale.ENGLISH.toLanguageTag()));
        builder.setTimezone(new Expression("UTC"));
        builder.setField(VariablePath.parse("field"));
        DateParser parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", parseDate);
        Assert.assertTrue(parse.process(event));
        Instant date = (Instant) event.get("field");
        Assert.assertEquals(expected, date);
    }

    @Test
    public void testNamedPatterns() throws ProcessorException {
        checkPattern("ISO_DATE_TIME", "2023-06-24T11:12:38.000001Z", Instant.ofEpochSecond(1687605158L, 1000L));
        checkPattern("ISO_INSTANT", "2023-06-24T11:12:38.000001Z", Instant.ofEpochSecond(1687605158L, 1000L));
        checkPattern("RFC_822_WEEK_DAY", "Tue, 1 Dec 2009 08:48:25 +0000", Instant.ofEpochSecond(1259657305L));
        checkPattern("RFC_822_WEEK_DAY", "Fri, 11 Dec 2009 08:48:25 +0000", Instant.ofEpochSecond(1260521305L));
        checkPattern("RFC_822_SHORT", "1 Dec 2009 08:48:25 +0000", Instant.ofEpochSecond(1259657305L));
        checkPattern("RFC_822_SHORT", "11 Dec 2009 08:48:25 +0000", Instant.ofEpochSecond(1260521305L));
        ZonedDateTime expected = Instant.ofEpochSecond(1259657305L).atZone(ZoneId.of("UTC")).with(ChronoField.YEAR, ZonedDateTime.now().get(ChronoField.YEAR));
        checkPattern("RFC_3164", "Dec 1 08:48:25", expected.toInstant());
        checkPattern("RFC_3164", "Dec 11 08:48:25", expected.with(ChronoField.DAY_OF_MONTH, 11).toInstant());
        Instant now = Instant.now();
        checkPattern("milliseconds", Long.toString(now.toEpochMilli()), Instant.ofEpochMilli(now.toEpochMilli()));
        checkPattern("seconds", Long.toString(now.getEpochSecond()), Instant.ofEpochSecond(now.getEpochSecond()));
        checkPattern("ISO8601", "2023-06-24T11:12:38Z", Instant.ofEpochSecond(1687605158L));
        checkPattern("UNIX_MS", Long.toString(now.toEpochMilli()), Instant.ofEpochMilli(now.toEpochMilli()));
        checkPattern("UNIX", Long.toString(now.getEpochSecond()), Instant.ofEpochSecond(now.getEpochSecond()));
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.DateParser"
                , BeanChecks.BeanInfo.build("locale", Expression.class)
                , BeanChecks.BeanInfo.build("timezone", Expression.class)
                , BeanChecks.BeanInfo.build("patterns", String[].class)
                , BeanChecks.BeanInfo.build("pattern", String.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", Object[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
