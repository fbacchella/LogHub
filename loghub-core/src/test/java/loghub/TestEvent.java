package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;

import loghub.EventsProcessor.ProcessingStatus;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.processors.Identity;

public class TestEvent {

    private static Logger logger;

    private static class Looper extends Processor {

        @Override
        public boolean process(Event event) {
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
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test
    public void testCanFork() throws ProcessorException {
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
    public void testCantFork() {
        Event e = factory.newTestEvent(new ConnectionContext<>() {
            {
                this.setPrincipal(() -> "loghub");
            }
            @Override
            public Object getLocalAddress() {
                return null;
            }

            @Override
            public Object getRemoteAddress() {
                return null;
            }
        });
        Pipeline ppl = new Pipeline(Collections.emptyList(), "main", "next");
        e.refill(ppl);
        e.put("key", "value");
        ProcessorException ex = Assert.assertThrows(ProcessorException.class, e::duplicate);
        Assert.assertTrue(ex.getMessage().startsWith("Unable to serialise event : "));
    }

    @Test
    public void testLoop() {
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
    public void testDrop() throws IOException, InterruptedException {
        String confile = "pipeline[main] { $dropper} pipeline[dropper] {[a] = true | drop}";
        Event ev = Tools.processEventWithPipeline(factory, confile, "main", e -> {});
        Assert.assertTrue((Boolean) ev.get("a"));
        Assert.assertEquals(1, Stats.getDropped());
        Assert.assertEquals(0, Stats.getInflight());
        Assert.assertEquals(0, Stats.getMetric(Meter.class, "main", "dropped").getCount());
        Assert.assertEquals(0, Stats.getMetric(Counter.class, "main", "inflight").getCount());
        Assert.assertEquals(1, Stats.getMetric(Meter.class, "dropper", "dropped").getCount());
        Assert.assertEquals(0, Stats.getMetric(Counter.class, "dropper", "inflight").getCount());
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

        event.put("a", 1);
        wrapped.put("b", 2);

        Assert.assertEquals(2, event.size());
        Assert.assertEquals(1, wrapped.size());

        Assert.assertEquals(1, event.get("a"));
        @SuppressWarnings("unchecked")
        Map<String, Object> values = (Map<String, Object>) event.get("wrapped");
        Assert.assertEquals(2, values.get("b"));
    }

    @Test
    public void testWrapEmptyPath() throws IOException, InterruptedException {
        String confile = "pipeline[main] {path[a]([.b]=1)}";
        Event received = Tools.processEventWithPipeline(factory, confile, "main", e -> {});
        Assert.assertFalse(received.containsKey("a"));
        Assert.assertTrue(received.containsKey("b"));
    }

    @Test
    public void testWrapEmptyProcessor() throws IOException, InterruptedException {
        String confile = "pipeline[main] {loghub.processors.Grok{pattern: \"a%{GREEDYDATA:a}\", field: [.message], path: [a]}}";
        Event received = Tools.processEventWithPipeline(factory, confile, "main", e -> e.put("message", "bc"));
        Assert.assertFalse(received.containsKey("a"));
        Assert.assertTrue(received.containsKey("message"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWrapDepthSuccess() {
        Event event = factory.newEvent();
        List<String> path = new ArrayList<>();
        for (String key : List.of("a", "b", "c", "d")) {
            path.add(key);
            VariablePath vp = VariablePath.of(path);
            Event wrap1 = event.wrap(VariablePath.of(vp));
            // Not created until first assignation
            Assert.assertNull(event.get("a"));
            Assert.assertEquals(NullOrMissingValue.MISSING, event.getAtPath(vp));
            wrap1.put("value", true);
            Assert.assertEquals(true, wrap1.get("value"));
            Assert.assertEquals(true, ((Map<String, Object>) event.getAtPath(vp)).get("value"));
            event.clear();
        }
    }

    @Test
    public void testWrapDepthFails() {
        Event event = factory.newEvent();
        List<String> path = new ArrayList<>();
        List<String> fullPath = List.of("a", "b", "c", "d");
        VariablePath fullVariablePath = VariablePath.of(fullPath);
        for (String key : fullPath) {
            path.add(key);
            VariablePath vp = VariablePath.of(path);
            event.putAtPath(vp, new Object());
            Assert.assertThrows(IgnoredEventException.class, () -> event.wrap(fullVariablePath));
            event.clear();
        }
    }

    @Test(timeout = 1000)
    public void testWrapperFailed() throws IOException, InterruptedException {
        String confile = "pipeline[main] {path[top a](path[d]([e]=false)) | [f]=true} ";
        Consumer<Event> evConsumer = e -> {
            Map<String, Object> wrapped = new HashMap<>((Map.of("b", NullOrMissingValue.NULL, "c", NullOrMissingValue.MISSING)));
            // Map.of doesn't allow null value;
            wrapped.put("a", null);
            e.put("top", wrapped);
            for (String key : List.of("a", "b", "c")) {
                Assert.assertThrows(IgnoredEventException.class, () -> e.wrap(VariablePath.of(List.of("top", key))));
            }
        };
        Event received = Tools.processEventWithPipeline(factory, confile, "main", evConsumer);
        Assert.assertFalse(received.containsAtPath(VariablePath.of(List.of("d"))));
        Assert.assertTrue((Boolean) received.getAtPath(VariablePath.of(List.of("f"))));
    }

}
