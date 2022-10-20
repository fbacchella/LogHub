package loghub.processors;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.axibase.date.PatternResolver;

import loghub.events.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestDateParser {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void test1() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("ISO_DATE_TIME");
        parse.setField(VariablePath.of("field"));
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
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-ddTHH:mm:ss.SSSZ");
        parse.setField(VariablePath.of("field"));
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
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSSSSXXX");
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", "1971-01-01T00:00:00.001001+01:00");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", "1970-12-31T23:00:00.001001Z", date.toString());
    }

    @Test
    public void test3() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss");
        parse.setTimezone("Z");
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", "1971-01-01T00:00:00");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", "1971-01-01T00:00:00Z", date.toString());
    }

    @Test
    public void test4() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", "Tue, 3 Jun 2008 11:05:30 +0110");
        parse.process(event);
        Instant date = (Instant) event.get("field");
        Assert.assertEquals("date not parsed", 1212486930000L, date.toEpochMilli());
    }

    @Test
    public void testIncomplete() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("MMM dd HH:mm:ss");
        parse.setTimezone("Z");
        parse.setField(VariablePath.of("field"));
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
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSxx");
        parse.setTimezone("CET");
        parse.setField(VariablePath.of("field"));
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
        DateParser parse = new DateParser();
        parse.setPattern("iso");
        parse.setTimezone("CET");
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", fieldValue);
        parse.process(event);
        Assert.assertEquals(fieldValue, event.get("field"));
    }

    @Test
    public void testFailedSyslog() throws ProcessorException {
        String fieldValue = "2016-08-04T18:57:37.238+0000";
        DateParser parse = new DateParser();
        parse.setPattern("MMM dd HH:mm:ss");
        parse.setTimezone("CET");
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", fieldValue);
        parse.process(event);
        Assert.assertEquals(fieldValue, event.get("field"));
    }

    @Test
    public void testAgain2() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("MMM dd HH:mm:ss.SSS");
        parse.setTimezone("CET");
        parse.setField(VariablePath.of("field"));
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
        DateParser processor = new DateParser();
        processor.setPattern("seconds");
        processor.setTimezone("CET");
        processor.setField(VariablePath.of("field"));
        Assert.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Function<Instant, Number> tonumber = (i) -> 1.0 * (i.getEpochSecond()) + i.getNano()/1e9;
        resolve(processor, Instant.ofEpochSecond(155, 330000000), tonumber);
        resolve(processor, Instant.ofEpochSecond(155, 30), tonumber);
        resolve(processor, Instant.ofEpochSecond(-155, 330000000), tonumber);
        resolve(processor, Instant.ofEpochSecond(-155, -330000000), tonumber);
        resolve(processor, Instant.ofEpochSecond(-155, 30), tonumber);
    }

    @Test
    public void testNumberInstantInteger() throws ProcessorException {
        DateParser processor = new DateParser();
        processor.setPattern("seconds");
        processor.setTimezone("CET");
        processor.setField(VariablePath.of("field"));
        Assert.assertTrue(processor.configure(new Properties(Collections.emptyMap())));

        Function<Instant, Number> tonumber = (i) -> i.getEpochSecond();
        resolve(processor, Instant.ofEpochSecond(155, 0), tonumber);
        resolve(processor, Instant.ofEpochSecond(-155, 0), tonumber);
    }

    @Test
    public void testNumberDate() throws ProcessorException {
        DateParser processor = new DateParser();
        processor.setPattern("milliseconds");
        processor.setTimezone("CET");
        processor.setField(VariablePath.of("field"));
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
    public void testBadPattern() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("failed");
        Assert.assertFalse(parse.configure(new Properties(Collections.emptyMap())));
    }

    private void checkTZ(String tz, String tzFormat, int expectedHour) throws ProcessorException {
        String parseformat = "yyyy MMM dd HH:mm:ss.SSS " + tzFormat;
        String parsedDate = "2019 Sep 18 07:53:09.504 " + tz;
        DateParser parse = new DateParser();
        parse.setPatterns(new String[] {parseformat});
        parse.setTimezone("America/Los_Angeles");
        parse.setField(VariablePath.of("field"));
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

}
