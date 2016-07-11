package loghub.processors;

import org.junit.Assert;
import org.junit.Test;

import loghub.Event;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestMapper {

    @Test
    public void test() throws ProcessorException, InterruptedException {
        Properties conf = Tools.loadConf("map.conf");
        for (Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", 1);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper"), conf);

        Assert.assertEquals("conversion not expected", "b", sent.get("a"));

    }

}
