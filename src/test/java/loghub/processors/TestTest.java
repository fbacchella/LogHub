package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;

public class TestTest {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Event", "loghub.EventInstance");
    }

    @Test
    public void testOK() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a",1);
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Assert.assertEquals("conversion not expected", 1, sent.get("b"));
    }

    @Test
    public void testKO() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }

        Event sent = Tools.getEvent();
        sent.put("a",2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Assert.assertEquals("conversion not expected", 2, sent.get("c"));
    }

    @Test(timeout=2000)
    public void testSub() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }

        Event sent = Tools.getEvent();
        sent.put("a",2);

        conf.mainQueue.add(sent);

        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        sent.inject(conf.namedPipeLine.get("subpipe"), conf.mainQueue);
        ep.start();

        Event received = conf.outputQueues.get("subpipe").take();
        Assert.assertEquals("conversion not expected", 2, received.get("d"));
        ep.interrupt();
    }

}
