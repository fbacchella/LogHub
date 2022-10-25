package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.events.Event;
import loghub.EventsProcessor;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

public class TestTest {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Event", "loghub.EventInstance");
    }

    @Test
    public void testOK() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a",1);
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Assert.assertEquals(1, sent.get("b"));
        Assert.assertEquals(1, sent.size());
    }

    @Test
    public void testKO() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        Helpers.parallelStartProcessor(conf);

        Event sent = factory.newEvent();
        sent.put("a",2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Assert.assertEquals(2, sent.get("c"));
        Assert.assertEquals(1, sent.size());
    }

    @Test
    public void testMissingPath() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("testclause.conf");
        Helpers.parallelStartProcessor(conf);

        Event sent = factory.newEvent();

        Tools.runProcessing(sent, conf.namedPipeLine.get("missingpath"), conf);
        Assert.assertEquals(2, sent.get("c"));
        Assert.assertEquals(1, sent.size());
    }

    @Test
    public void testSub() throws ProcessorException, ConfigException, IOException {
        Stats.reset();
        Properties conf = Tools.loadConf("testclause.conf");
        Helpers.parallelStartProcessor(conf);

        Event sent = factory.newEvent();
        sent.put("a", 2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("subpipe"), conf);
        System.out.println(sent);
        Assert.assertEquals(2, sent.get("c"));
        Assert.assertEquals(2, sent.get("d"));
        Assert.assertEquals(2, sent.size());
    }

}
