package loghub.processors;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestDateParser {

    private static Logger logger;

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
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.now()));
        parse.process(event);
        assertTrue("date not parsed", event.get("field") instanceof Date);
    }

    @Test
    public void test2() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSSSSXXX");
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "1970-01-01T00:00:00.000000+01:00");
        parse.process(event);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", -3600000, date.getTime());
    }

    @Test
    public void test3() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss");
        parse.setTimezone("Z");
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "1970-01-01T00:00:00");
        parse.process(event);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", 0L, date.getTime());
    }

    @Test
    public void test4() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "Tue, 3 Jun 2008 11:05:30 +0110");
        parse.process(event);
        Assert.assertTrue("date not parsed", event.get("field") instanceof Date);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", 1212486930000L, date.getTime());
    }

    @Test
    public void testIncomplete() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("MMM dd HH:mm:ss");
        parse.setTimezone("Z");
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "Jul 26 16:40:22");
        parse.process(event);
        Date date = (Date) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date.toInstant(), ZoneId.of("GMT"));
        int year = OffsetDateTime.now().get(ChronoField.YEAR);
        Assert.assertEquals("date not parsed", year, t.getLong(ChronoField.YEAR));
    }

    @Test
    public void testAgain() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSxx");
        parse.setTimezone("CET");
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "2016-08-04T18:57:37.238+0000");
        parse.process(event);
        Date date = (Date) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date.toInstant(), ZoneId.of("GMT"));
        Assert.assertEquals("date not parsed", 18, t.getLong(ChronoField.HOUR_OF_DAY));
    }

    @Test
    public void testAgain2() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("MMM dd HH:mm:ss.SSS");
        parse.setTimezone("CET");
        parse.setField("field");
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", "Jul 26 16:40:22.238");
        parse.process(event);
        Date date = (Date) event.get("field");
        OffsetDateTime t = OffsetDateTime.ofInstant(date.toInstant(), ZoneId.of("GMT"));
        Assert.assertEquals("date not parsed", 14, t.getLong(ChronoField.HOUR_OF_DAY));
    }

    @Test
    public void testBadPattern() throws ProcessorException {
        DateParser parse = new DateParser();
        parse.setPattern("failed");
        Assert.assertFalse(parse.configure(new Properties(Collections.emptyMap())));
    }

}
