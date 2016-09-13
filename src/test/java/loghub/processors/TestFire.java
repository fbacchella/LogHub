package loghub.processors;

import java.io.IOException;

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

public class TestFire {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void test() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("fire.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("count", 2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event old = conf.mainQueue.remove();
        Event newevent = conf.mainQueue.remove();
        Assert.assertEquals("Not matching old event", old.get("count"), 2);
        Assert.assertEquals("Event not fired", 6, newevent.get("c"));

    }

}
