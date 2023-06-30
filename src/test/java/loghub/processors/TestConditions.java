package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestConditions {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression");
    }

    @Test
    public void testif() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("conditions.conf");
        Event sent = factory.newEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("ifpipe"), conf);

        Assert.assertEquals("conversion not expected", String.class, sent.get("a").getClass());
    }

    @Test
    public void testsuccess() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = factory.newEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("successpipe"), conf);

        Assert.assertEquals("conversion not expected", "success", sent.get("test"));
    }

    @Test
    public void testfailure() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = factory.newEvent();
        sent.put("a", "a");
        Tools.runProcessing(sent, conf.namedPipeLine.get("failurepipe"), conf);
        Assert.assertNull(sent.getLastException());
        Assert.assertEquals("a", sent.get("a"));
        Assert.assertEquals("failure", sent.get("test"));
        Assert.assertEquals("failure", sent.get("test"));
        Assert.assertEquals("Field with path \"[a]\" invalid: Unable to parse \"a\" as a java.lang.Integer: For input string: \"a\"", sent.get("lastException"));
    }

    @Test
    public void testsubpipe() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = factory.newEvent();
        sent.put("a", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("subpipe"), conf);
        Assert.assertEquals("sup pipeline not processed", 1, sent.get("b"));
        Assert.assertEquals("sup pipeline not processed", 2, sent.get("c"));
    }

    @Test
    public void testignored() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("conditions.conf");

        Event sent = factory.newEvent();
        sent.put("z", "1");

        Tools.runProcessing(sent, conf.namedPipeLine.get("ignore"), conf);
        Assert.assertEquals("success was called", null, sent.get("b"));
        Assert.assertEquals("failure was called", null, sent.get("c"));
        Assert.assertEquals("exception was called", null, sent.get("d"));
        Assert.assertEquals("Event was processed, when it should not have been", "1", sent.get("z"));
    }

}
