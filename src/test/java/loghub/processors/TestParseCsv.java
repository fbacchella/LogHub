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
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;

public class TestParseCsv {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void test1() throws ProcessorException {
        ParseCsv parse = new ParseCsv();
        parse.setHeaders(new String[]{"a", "b", "c", "d"});
        parse.setField(VariablePath.of(new String[] {"message"}));
        parse.setColumnSeparator(';');
        parse.setFeatures(new String[]{"TRIM_SPACES"});
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("message", "1; \"2\";\\\";");
        parse.process(event);
        Assert.assertEquals("1", event.get("a"));
        Assert.assertEquals("2", event.get("b"));
        Assert.assertEquals("\\\"", event.get("c"));
        Assert.assertEquals(null, event.get("d"));
    }

    @Test
    public void test_loghub_processors_ParseCsv() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.processors.ParseCsv"
                              ,BeanChecks.BeanInfo.build("headers", BeanChecks.LSTRING)
                              ,BeanChecks.BeanInfo.build("columnSeparator", Character.class)
                              ,BeanChecks.BeanInfo.build("nullValue", String.class)
                              ,BeanChecks.BeanInfo.build("features", BeanChecks.LSTRING)
                              ,BeanChecks.BeanInfo.build("escapeChar", Character.TYPE)
                        );
    }

}
