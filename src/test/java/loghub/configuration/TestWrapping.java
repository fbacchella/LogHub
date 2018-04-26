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

import loghub.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;

public class TestWrapping {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Event", "loghub.EventsProcessor");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSourceLoading() throws ConfigException, IOException, ProcessorException, InterruptedException {
        Properties conf = Tools.loadConf("wrap.conf");
        Event ev = Event.emptyEvent(null);
        ev.put("a", new HashMap<Object, Object>());
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ev.inject(conf.namedPipeLine.get("main"), conf.mainQueue);
        ep.start();
        Event processed = conf.outputQueues.get("main").poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("b", ((Map<String, Object>)processed.get("a")).get("c"));
        ep.stopProcessing();
    }

}
