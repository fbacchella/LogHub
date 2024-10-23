package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Helpers;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestFire {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE);
    }

    @Test
    public void test() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("fire.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("count", 2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event old = conf.mainQueue.remove();
        Event newevent = conf.mainQueue.remove();
        Assert.assertEquals("Not matching old event", old.get("count"), 2);
        Assert.assertEquals("Event not fired", 6, newevent.get("c"));

    }

}
