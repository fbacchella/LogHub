package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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

public class TestSplit {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testConversion() throws ProcessorException {
        var r1 = test(",", true, "a,b,c");
        Assert.assertEquals("[a, b, c]", r1.toString());
        r1 = test(",", true,",a,b,c,");
        Assert.assertEquals("[, a, b, c, ]", r1.toString());
        r1 = test(",", false,",a,b,c,");
        Assert.assertEquals("[a, b, c]", r1.toString());
    }

    private List<String> test(String Pattern, boolean keepempty, String message) throws ProcessorException {
        Split parse = new Split();
        parse.setPattern(",");
        parse.setKeepempty(keepempty);
        parse.setField(VariablePath.of("field"));
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("field", message);
        parse.process(event);
        return (List<String>) event.get("field");
    }

    @Test
    public void test_loghub_processors_Crlf() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Crlf"
                , BeanChecks.BeanInfo.build("regex", String.class)
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
