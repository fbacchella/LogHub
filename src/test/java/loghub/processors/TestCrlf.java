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
import loghub.events.Event;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;

public class TestCrlf {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testConversion() throws ProcessorException {
        test(Crlf.Format.CRLF, false, "a\rb\nc\r\nd", "a\r\nb\r\nc\r\nd");
        test(Crlf.Format.CRLF, true, "a\rb\nc\r\nd", "a\\r\\nb\\r\\nc\\r\\nd");
        test(Crlf.Format.CR, false, "a\rb\nc\r\nd", "a\rb\rc\rd");
        test(Crlf.Format.CR, true, "a\rb\nc\r\nd", "a\\rb\\rc\\rd");
        test(Crlf.Format.LF, false, "a\rb\nc\r\nd", "a\nb\nc\nd");
        test(Crlf.Format.LF, true, "a\rb\nc\r\nd", "a\\nb\\nc\\nd");
        test(Crlf.Format.KEEP, true, "a\rb\nc\r\nd", "a\\rb\\nc\\r\\nd");
    }

    private void test(Crlf.Format format, boolean escape, String input, String output) throws ProcessorException {
        Crlf.Builder builder = Crlf.getBuilder();
        builder.setEscape(escape);
        builder.setFormat(format);
        Crlf parse = new Crlf(builder);
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", input);
        parse.process(event);
        Assert.assertEquals(output, event.get("field"));
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Crlf"
                , BeanChecks.BeanInfo.build("format", Crlf.Format.class)
                , BeanChecks.BeanInfo.build("escape", Boolean.TYPE)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", new Object[] {}.getClass())
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }
}
