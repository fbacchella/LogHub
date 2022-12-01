package loghub;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.EventsProcessor.ProcessingStatus;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Identity;

public class TestEvent {

    private static Logger logger ;

    private static class Looper extends Processor {

        @Override
        public boolean process(Event event) throws ProcessorException {
            event.appendProcessor(this);
            return true;
        }

        @Override
        public String getName() {
            return "Looper";
        }

    }

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test
    public void TestForkable() {
        Event e = factory.newTestEvent();
        Pipeline ppl = new Pipeline(Collections.emptyList(), "main", "next");
        e.refill(ppl);
        e.put("key", "value");
        Event e2 = e.duplicate();
        Assert.assertNotNull(e2);
        Pipeline newPipe = new Pipeline(Collections.singletonList(new Identity()), "next", null);
        e2.refill(newPipe);
        Assert.assertEquals("cloned value not found", e.get("key"), e2.get("key"));
        e.end();
        e2.end();
    }

    @Test
    public void TestLoop() {
        Pipeline ppl = new Pipeline(List.of(new Looper(), new Looper(), new Looper(), new Looper(), new Looper(), new Looper()), "main", null);
        Map<String, Object> conf = new HashMap<>();
        conf.put("maxSteps", 5);
        Properties props = new Properties(conf);
        Event e = factory.newTestEvent();
        e.refill(ppl);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, props.maxSteps, props.repository);
        Processor processor;
        int numsteps = 0;
        int loop = 0;
        while ((processor = e.next()) != null) {
            logger.debug("doing step");
            loop++;
            if (ep.process(e, processor) != ProcessingStatus.CONTINUE) {
                break;
            }
            Assert.assertTrue("Not counting processing", e.processingDone() > numsteps);
            Assert.assertTrue("Not stopping processing", e.processingDone() <= props.maxSteps);
            Assert.assertTrue("Not stopping processing", e.processingDone() <= loop);
            numsteps = e.processingDone();
        }
        Assert.assertTrue("Breaking early", e.processingDone() >= props.maxSteps);
        e.end();
    }

    @Test
    public void testWrapper() {
        Event event = factory.newEvent();
        Event wrapped = event.wrap(VariablePath.of(List.of("wrapped")));
        wrapped.putMeta("a", 1);
        Assert.assertEquals(1, wrapped.getMeta("a"));
        Assert.assertEquals(1, event.getMeta("a"));
        event.putMeta("b", 2);
        Assert.assertEquals(2, wrapped.getMeta("b"));
        Assert.assertEquals(2, event.getMeta("b"));

        Assert.assertEquals(2, wrapped.getMetas().size());
        Assert.assertEquals(2, event.getMetas().size());
    }

}
