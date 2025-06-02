package loghub;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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
import loghub.senders.BlockingConnectionContext;

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
        String confile = "pipeline[main] {[a] = true | drop}";
        Properties props = Tools.loadConf(new StringReader(confile));
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, 100, props.repository);
        ep.start();
        BlockingConnectionContext ctx = new BlockingConnectionContext();
        Event ev = factory.newEvent(ctx);
        ev.inject(props.namedPipeLine.get("main"), props.mainQueue, true);
        boolean computed = ctx.getLocalAddress().tryAcquire(5, TimeUnit.SECONDS);
        Assert.assertTrue(computed);
        ep.interrupt();
        ep.join();
        Assert.assertTrue((Boolean) ev.get("a"));
        Assert.assertEquals(1, Stats.getDropped());
        Assert.assertEquals(0, Stats.getInflight());
        Assert.assertEquals(1, Stats.getMetric(Meter.class, "main", "dropped").getCount());
        Assert.assertEquals(0, Stats.getMetric(Counter.class, "main", "inflight").getCount());
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
    public void testWrapEmptyPath() throws IOException {
        String confile = "pipeline[main] {path[a]([.b]=1)}";
        Properties conf = Tools.loadConf(new StringReader(confile));
        Event sent = factory.newEvent();
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event received = conf.mainQueue.remove();
        Assert.assertFalse(received.containsKey("a"));
        Assert.assertTrue(received.containsKey("b"));
    }

    @Test
    public void testWrapEmptyProcessor() throws IOException {
        String confile = "pipeline[main] {loghub.processors.Grok{pattern: \"a%{GREEDYDATA:a}\", field: [.message], path: [a]}}";
        Properties conf = Tools.loadConf(new StringReader(confile));
        Event sent = factory.newEvent();
        sent.put("message", "bc");
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event received = conf.mainQueue.remove();
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
        BlockingConnectionContext waitingContext = new BlockingConnectionContext();
        Event event = factory.newEvent(waitingContext);
        Map<String, Object> wrapped = new HashMap<>((Map.of("b", NullOrMissingValue.NULL, "c", NullOrMissingValue.MISSING)));
        // Map.of doesn't allow null value;
        wrapped.put("a", null);
        event.put("top", wrapped);
        for (String key : List.of("a", "b", "c")) {
            Assert.assertThrows(IgnoredEventException.class, () -> event.wrap(VariablePath.of(List.of("top", key))));
        }
        // Should not neither descend nor create [d e]
        String confile = "pipeline[main] {path[top a](path[d]([e]=false)) | [f]=true} ";
        Properties props = Tools.loadConf(new StringReader(confile));
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, 100, props.repository);
        event.inject(props.namedPipeLine.get("main"), props.mainQueue, false);
        props.mainQueue.put(event);
        ep.start();
        waitingContext.getLocalAddress().acquire();
        ep.stopProcessing();
        Assert.assertFalse(event.containsAtPath(VariablePath.of(List.of("d"))));
        Assert.assertTrue((Boolean) event.getAtPath(VariablePath.of(List.of("f"))));
    }

}
