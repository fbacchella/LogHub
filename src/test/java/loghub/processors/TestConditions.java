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
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestConditions {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
    }

    @Test
    public void testif() throws ProcessorException {
        Properties conf = Tools.loadConf("conditions.conf");
        Event sent = Tools.getEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("ifpipe"), conf);

        Assert.assertEquals("conversion not expected", String.class, sent.get("a").getClass());
    }

    @Test
    public void testsuccess() throws ProcessorException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = Tools.getEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("successpipe"), conf);

        Assert.assertEquals("conversion not expected", "success", sent.get("test"));
    }

    @Test
    public void testfailure() throws InterruptedException, ProcessorException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = Tools.getEvent();
        sent.put("a", "a");

        Tools.runProcessing(sent, conf.namedPipeLine.get("failurepipe"), conf);

        Assert.assertEquals("conversion not expected", "failure", sent.get("test"));
    }

    @Test
    public void testsubpipe() throws InterruptedException, ProcessorException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = Tools.getEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("subpipe"), conf);
        Assert.assertEquals("sup pipeline not processed", 1, sent.get("b"));
        Assert.assertEquals("sup pipeline not processed", 2, sent.get("c"));
    }

}
