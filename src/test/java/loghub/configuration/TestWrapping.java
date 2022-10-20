package loghub.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.events.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.events.EventsFactory;

public class TestWrapping {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Event", "loghub.EventsProcessor", "loghub");
    }

    @Test
    public void testSourceLoadingContains() throws ConfigException, IOException, ProcessorException, InterruptedException {
        Event ev = factory.newEvent();
        ev.put("a", new HashMap<String, Object>());
        checkEvent(ev);
    }

    @Test
    public void testSourceLoadingNotContains() throws ConfigException, IOException, ProcessorException, InterruptedException {
        Event ev = factory.newEvent();
        checkEvent(ev);
    }
    
    @SuppressWarnings("unchecked")
    private void checkEvent(Event ev) throws ConfigException, IOException, InterruptedException {
        Properties conf = Tools.loadConf("wrap.conf");
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ev.inject(conf.namedPipeLine.get("main"), conf.mainQueue, false);
        ep.start();
        Event processed = conf.outputQueues.get("main").poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("b", ((Map<String, Object>)processed.get("a")).get("c"));
        Assert.assertEquals(1, processed.getMeta("d"));
        Assert.assertEquals(0L, processed.getTimestamp().getTime());
        Assert.assertEquals("b", processed.get("e"));
        Assert.assertEquals(1, ((Map<String, Object>)processed.get("a")).get("#f"));
        Assert.assertEquals(2, ((Map<String, Object>)processed.get("a")).get("@timestamp"));
        ep.stopProcessing();
    }

}
