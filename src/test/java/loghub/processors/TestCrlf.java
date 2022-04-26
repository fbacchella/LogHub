package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Event;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;

public class TestCrlf {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testConversion() throws ProcessorException {
        test("CRlf", false, "a\rb\nc\r\nd", "a\r\nb\r\nc\r\nd");
        test("CRlf", true, "a\rb\nc\r\nd", "a\\r\\nb\\r\\nc\\r\\nd");
        test("CR", false, "a\rb\nc\r\nd", "a\rb\rc\rd");
        test("CR", true, "a\rb\nc\r\nd", "a\\rb\\rc\\rd");
        test("LF", false, "a\rb\nc\r\nd", "a\nb\nc\nd");
        test("lf", true, "a\rb\nc\r\nd", "a\\nb\\nc\\nd");
        test(null, false, "a\rb\nc\r\nd", "a\rb\nc\r\nd");
        test("", true, "a\rb\nc\r\nd", "a\\rb\\nc\\r\\nd");
        test("kEEp", true, "a\rb\nc\r\nd", "a\\rb\\nc\\r\\nd");
    }

    private void test(String format, boolean escape, String input, String output) throws ProcessorException {
        Crlf parse = new Crlf();
        parse.setEscape(escape);
        parse.setFormat(format);
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", input);
        parse.process(event);
        Assert.assertEquals(output, event.get("field"));
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Crlf"
                , BeanChecks.BeanInfo.build("format", String.class)
                , BeanChecks.BeanInfo.build("escape", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", String.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", new Object[] {}.getClass())
                , BeanChecks.BeanInfo.build("path", String.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }
}
