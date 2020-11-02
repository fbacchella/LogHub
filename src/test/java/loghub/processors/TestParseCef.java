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
import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestParseCef {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    public void testCef() throws ProcessorException {
        ParseCef parse = new ParseCef();
        parse.setField(new String[]{"content"});
        Assert.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = Tools.getEvent();
        event.put("content", "CEF:0|security|threatmanager|1.0|100|detected a \\| in packet|10|src=10.0.0.1 act=blocked a \\\\ dst=1.1.1.1 comment=with | in it comment2=with \\= in it comment3=with \\r in it  app=3");
        Assert.assertTrue(parse.process(event));
        @SuppressWarnings("unchecked")
        Map<String, Object> fields= (Map<String, Object>) event.get("cef_fields");
        String[] fields_names = new String[]{"version", "device_vendor", "device_product", "device_version", "device_event_class_id", "name", "severity"};
        Object[] fields_values = new Object[]{0, "security", "threatmanager", "1.0", "100", "detected a | in packet", 10};
        for (int i=0; i < fields_names.length; i++) {
            Assert.assertEquals(fields_values[i], fields.get(fields_names[i]));
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> extensions= (Map<String, Object>) event.get("cef_extensions");
        String[] extensions_names = new String[]{"src", "act", "dst", "comment", "comment2", "comment3", "app"};
        Object[] extensions_values = new Object[]{"10.0.0.1", "blocked a \\", "1.1.1.1", "with | in it", "with = in it", "with \r in it", "3"};
        for (int i=0; i < extensions_names.length; i++) {
            Assert.assertEquals(extensions_values[i], extensions.get(extensions_names[i]));
        }
    }

    @Test
    public void test_loghub_processors_ParseCsv() throws ClassNotFoundException, IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.processors.ParseCef"
                              ,BeanChecks.BeanInfo.build("field", BeanChecks.LSTRING)
                        );
    }

}
