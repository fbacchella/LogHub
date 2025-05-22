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
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.RouteParser;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestParseCsv {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void test1() throws ProcessorException {
        ParseCsv.Builder builder = ParseCsv.getBuilder();
        builder.setHeaders(
                List.of(VariablePath.of("a"), VariablePath.of("b"), VariablePath.of("c"), VariablePath.of("d"))
                    .toArray(VariablePath[]::new)
        );
        builder.setField(VariablePath.of("message"));
        builder.setColumnSeparator(';');
        builder.setFeatures(new String[]{"TRIM_SPACES"});
        ParseCsv parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message", "1; \"2\";\\\";");
        parse.process(event);
        Assert.assertEquals("1", event.get("a"));
        Assert.assertEquals("2", event.get("b"));
        Assert.assertEquals("\\\"", event.get("c"));
        Assert.assertNull(event.get("d"));
    }

    @Test
    public void testParseVp() throws ProcessorException {
        String confFragment = "loghub.processors.ParseCsv { columnSeparator: ';', headers: [[a],[b]], nullValue: null, }";
        ParseCsv parser = ConfigurationTools.unWrap(confFragment, RouteParser::object);
        Assert.assertTrue(parser.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message","1;\"2\"");
        parser.process(event);
        Assert.assertEquals("1", event.get("a"));
        Assert.assertEquals("2", event.get("b"));
    }

    @Test
    public void testParseString() throws ProcessorException {
        String confFragment = "loghub.processors.ParseCsv { columnSeparator: ';', headers: [\"a\", \"b\"], nullValue: null, }";
        ParseCsv parser = ConfigurationTools.unWrap(confFragment, RouteParser::object);
        Assert.assertTrue(parser.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("message","1;\"2\"");
        parser.process(event);
        Assert.assertEquals("1", event.get("a"));
        Assert.assertEquals("2", event.get("b"));
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.ParseCsv"
                              , BeanChecks.BeanInfo.build("headers", VariablePath[].class)
                              , BeanChecks.BeanInfo.build("columnSeparator", Character.TYPE)
                              , BeanChecks.BeanInfo.build("nullValue", String.class)
                              , BeanChecks.BeanInfo.build("features", BeanChecks.LSTRING)
                              , BeanChecks.BeanInfo.build("escapeChar", Character.TYPE)
                        );
    }

}
