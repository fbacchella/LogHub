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
        System.out.println(sent);
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
        System.out.println(sent);
        Assert.assertEquals("conversion not expected", "b", sent.get("a"));

    }

    @Test
    public void test3() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 3);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        System.out.println(sent);
        Assert.assertEquals("conversion not expected", 3, sent.get("a"));

    }

}
