package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

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
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestParseCef {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    private void check(Map<String, Object> fields) {
        String[] fields_names = new String[]{"version", "device_vendor", "device_product", "device_version", "device_event_class_id", "name", "severity"};
        Object[] fields_values = new Object[]{0, "security", "threatmanager", "1.0", "100", "detected a | in packet", 10};
        for (int i=0; i < fields_names.length; i++) {
            Assert.assertEquals(fields_values[i], fields.get(fields_names[i]));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> extensions= (Map<String, Object>) fields.get("extensions");
        String[] extensions_names = new String[]{"src", "act", "dst", "comment", "comment2", "comment3", "comment4", "app"};
        Object[] extensions_values = new Object[]{"10.0.0.1", "blocked a \\", "1.1.1.1", "with | in it", "with = in it", "with \n in it", "with \r in it", "3"};
        for (int i=0; i < extensions_names.length; i++) {
            Assert.assertEquals(extensions_values[i], extensions.get(extensions_names[i]));
        }
    }

    @Test
    public void testCef() throws ProcessorException {
        ParseCef.Builder builder = ParseCef.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setDestination(VariablePath.of("cef"));
        ParseCef parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("message", "CEF:0|security|threatmanager|1.0|100|detected a \\| in packet|10|src=10.0.0.1 act=blocked a \\\\ dst=1.1.1.1 comment=with | in it comment2=with \\= in it comment3=with \\n in it comment4=with \\r in it  app=3");
        Assert.assertTrue(parse.process(event));
        @SuppressWarnings("unchecked")
        Map<String, Object> fields = (Map<String, Object>) event.get("cef");
        check(fields);
    }

    @Test
    public void testCefInPlace() throws ProcessorException {
        ParseCef.Builder builder = ParseCef.getBuilder();
        builder.setField(VariablePath.of("message"));
        builder.setInPlace(true);
        ParseCef parse = builder.build();
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.putAtPath(VariablePath.of("message"), "CEF:0|security|threatmanager|1.0|100|detected a \\| in packet|10|src=10.0.0.1 act=blocked a \\\\ dst=1.1.1.1 comment=with | in it comment2=with \\= in it comment3=with \\n in it comment4=with \\r in it  app=3");
        Assert.assertTrue(parse.process(event));
        check(event);
    }

    @Test
    public void test_loghub_processors_ParseCsv() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.ParseCef"
                                    , BeanChecks.BeanInfo.build("field", VariablePath.class)
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
