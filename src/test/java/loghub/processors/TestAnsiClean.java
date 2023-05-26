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
import loghub.Expression;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestAnsiClean {

    private final EventsFactory factory = new EventsFactory();
    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.AnsiClean");
    }

    private Event resolve(String replacement, Object message) throws ProcessorException {
        AnsiClean.Builder builder = AnsiClean.getBuilder();
        builder.setReplacement(replacement);
        AnsiClean cv = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(cv.configure(props));

        Event e = factory.newEvent();
        e.put("message", message);
        e.process(cv);
        return e;
    }

    @Test
    public void testAnsiCleanReplaced() throws ProcessorException {
        Event ev = resolve("<ANSI>", "\u001b[0;33mmessage1\u001b[0mmessage2\u001b[0mmessage3");
        Assert.assertEquals("<ANSI>message1<ANSI>message2<ANSI>message3", ev.get("message"));
    }

    @Test
    public void testAnsiClean() throws ProcessorException {
        Event ev = resolve("", "\u001b[0;33mmessage1\u001b[0mmessage2\u001b[0mmessage3");
        Assert.assertEquals("message1message2message3", ev.get("message"));
    }

    @Test
    public void testAnsiNull() throws ProcessorException {
        Event ev = resolve("", null);
        Assert.assertNull(ev.get("message"));
    }

    @Test
    public void testAnsiNullOrMissingValue() throws ProcessorException {
        Event ev = resolve("", NullOrMissingValue.MISSING);
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.get("message"));
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.AnsiClean"
                , BeanChecks.BeanInfo.build("replacement", String.class)
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
