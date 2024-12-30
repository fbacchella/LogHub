package loghub.processors;

import java.beans.IntrospectionException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestSplit {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testConversion() throws ProcessorException {
        var r1 = test(",", true, "a,b,c");
        Assert.assertEquals("[a, b, c]", r1.toString());
        r1 = test(",", true, ",a,b,c,");
        Assert.assertEquals("[, a, b, c, ]", r1.toString());
        r1 = test(",", false, ",a,b,c,");
        Assert.assertEquals("[a, b, c]", r1.toString());
        r1 = test("#", false, "#a#b#c#");
        Assert.assertEquals("[a, b, c]", r1.toString());
        r1 = test(",", false, "a");
        Assert.assertEquals(List.of("a"), r1);
    }

    @SuppressWarnings("unchecked")
    private List<String> test(String pattern, boolean keepempty, String message) throws ProcessorException {
        Split.Builder builder = Split.getBuilder();
        builder.setPattern(pattern);
        builder.setKeepempty(keepempty);
        Split parse = builder.build();
        parse.setField(VariablePath.parse("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", message);
        parse.process(event);
        return (List<String>) event.get("field");
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Split"
                , BeanChecks.BeanInfo.build("pattern", String.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
