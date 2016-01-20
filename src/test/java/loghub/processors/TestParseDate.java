package loghub.processors;

import static org.junit.Assert.assertTrue;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class TestParseDate {

    @Test
    public void test1() throws ProcessorException {
        ParseDate parse = new ParseDate();
        parse.setPattern("ISO_INSTANT");
        parse.setField("field");
        parse.configure(new Properties(Collections.emptyMap()));
        Event event = new Event();
        event.put("field", DateTimeFormatter.ISO_INSTANT.format(ZonedDateTime.now()));
        parse.process(event);
        assertTrue("date not parsed", event.get("field") instanceof Date);
    }

    @Test
    public void test2() throws ProcessorException {
        ParseDate parse = new ParseDate();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss.SSSSSSXXX");
        parse.setField("field");
        parse.configure(new Properties(Collections.emptyMap()));
        Event event = new Event();
        event.put("field", "1970-01-01T00:00:00.000000+01:00");
        parse.process(event);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", -3600000, date.getTime());
    }

    @Test
    public void test3() throws ProcessorException {
        ParseDate parse = new ParseDate();
        parse.setPattern("yyyy-MM-dd'T'HH:m:ss");
        parse.setTimeZone("Z");
        parse.setField("field");
        parse.configure(new Properties(Collections.emptyMap()));
        Event event = new Event();
        event.put("field", "1970-01-01T00:00:00");
        parse.process(event);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", 0L, date.getTime());
    }

    @Test
    public void test4() throws ProcessorException {
        ParseDate parse = new ParseDate();
        parse.setField("field");
        parse.configure(new Properties(Collections.emptyMap()));
        Event event = new Event();
        event.put("field", "Tue, 3 Jun 2008 11:05:30 GMT");
        parse.process(event);
        Assert.assertTrue("date not parsed", event.get("field") instanceof Date);
        Date date = (Date) event.get("field");
        Assert.assertEquals("date not parsed", 1212491130000L, date.getTime());
    }

}
