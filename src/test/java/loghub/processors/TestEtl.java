package loghub.processors;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestEtl {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
    }

    @Test
    public void test1() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue("a.b");
        etl.setExpression("event.c + 1");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("c", 0);
        event.process(etl);
        Assert.assertEquals("evaluation failed", 1, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a", "b"}, null, false));
    }

    @Test
    public void test2() throws ProcessorException {
        Etl etl = new Etl.Remove();
        etl.setLvalue("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", null, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a"}, null, false));
    }

    @Test
    public void test3() throws ProcessorException {
        Etl.Rename etl = new Etl.Rename();
        etl.setLvalue("b");
        etl.setSource("a");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 0, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"b"}, null, false));
    }

    @Test
    public void test4() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue("a");
        etl.setExpression("formatters.a.format(event)");
        Map<String, String> formats = Collections.singletonMap("a", "${@timestamp%t<GMT>H}");
        Map<String, Object> properties = new HashMap<>();
        properties.put("__FORMATTERS", formats);
        boolean done = etl.configure(new Properties(properties));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.setTimestamp(new Date(3600 * 1000));
        event.process(etl);
        Assert.assertEquals("evaluation failed", "01", event.get("a"));
    }

    @Test
    public void test5() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", "a");

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);

        Assert.assertEquals("conversion not expected", "a", sent.get("a"));
    }

    @Test
    public void test6() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("count", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("second"), conf);
        Assert.assertEquals("conversion not expected", 1, sent.get("count"));

    }

}
