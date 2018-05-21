package loghub.processors;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestMapper {

    @Test
    public void test1() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 1);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper1"), conf);
        Assert.assertEquals("conversion not expected", "b", sent.get("d"));
    }

    @Test
    public void test2() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 1);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", "b", sent.get("a"));
    }

    @Test
    public void testNotMapped() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 3);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", 3, sent.get("a"));
    }

    @Test
    public void test4() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 2L);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", "c", sent.get("a"));
    }

    @Test
    public void testExpression() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper3"), conf);
        Assert.assertEquals("conversion not expected", "c", sent.get("a"));
    }

}
