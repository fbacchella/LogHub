package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import loghub.events.Event.Action;
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
    public void TestPath() {
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.applyAtPath(Action.PUT, VariablePath.of(new String[]{"a", "b", "c"}), 1, true);
        e.put("d", 2);
        e.applyAtPath(Action.PUT, VariablePath.of(new String[]{"e"}), 3, true);
        e.applyAtPath(Action.PUT, VariablePath.ofMeta("f"), 4, true);
        Assert.assertEquals("wrong number of keys", 3, e.keySet().size());
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.applyAtPath(Action.GET, VariablePath.of(new String[]{"a", "b", "c"}), null));
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.applyAtPath(Action.REMOVE, VariablePath.of(new String[]{"a", "b", "c"}), null));
        Assert.assertEquals("Didn't resolve the path correctly",  2, e.applyAtPath(Action.GET, VariablePath.of(new String[]{"d"}), null) );
        Assert.assertEquals("Didn't resolve the path correctly",  4, e.getMeta("f") );
        Assert.assertEquals("Didn't resolve the path correctly",  3, e.get("e") );
        Assert.assertEquals("Didn't resolve the path correctly",  new Date(0), e.applyAtPath(Action.GET, VariablePath.TIMESTAMP, null));
    }

    @Test
    public void testAppend() {
        Event e = factory.newEvent();

        e.put("a", "1");
        applyAction(e, false, Action.APPEND, "2", "a");

        e.put("a", null);
        applyAction(e, true, Action.APPEND, "1", "a");
        Assert.assertEquals(List.of("1"), e.get("a"));

        e.put("a", new ArrayList<>(List.of("1")));
        applyAction(e, true, Action.APPEND, "2", "a");
        Assert.assertEquals(List.of("1", "2"), e.get("a"));

        e.put("a", new char[]{'1'});
        applyAction(e, true, Action.APPEND, '2', "a");
        Assert.assertArrayEquals(new char[]{'1', '2'}, (char[])e.get("a"));

        e.put("a", new boolean[]{true});
        applyAction(e, true, Action.APPEND, false, "a");
        Assert.assertArrayEquals(new boolean[]{true, false}, (boolean[])e.get("a"));

        e.put("a", new byte[]{1});
        applyAction(e, true, Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new byte[]{1, 2}, (byte[])e.get("a"));

        e.put("a", new short[]{1});
        applyAction(e, true, Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new short[]{1, 2}, (short[])e.get("a"));

        e.put("a", new int[]{1});
        applyAction(e, true, Action.APPEND, 2, "a");
        Assert.assertArrayEquals(new int[]{1, 2}, (int[])e.get("a"));

        e.put("a", new long[]{1L});
        applyAction(e, true, Action.APPEND, 2L, "a");
        Assert.assertArrayEquals(new long[]{1L, 2L}, (long[])e.get("a"));

        e.put("a", new float[]{1.0f});
        applyAction(e, true, Action.APPEND, 2.0f, "a");
        Assert.assertArrayEquals(new float[]{1.0f, 2.0f}, (float[])e.get("a"), 1e-5f);

        e.put("a", new double[]{1.0});
        applyAction(e, true, Action.APPEND, 2.0, "a");
        Assert.assertArrayEquals(new double[]{1.0, 2.0}, (double[])e.get("a"), 1e-5);

        e.put("a", new String[]{"1"});
        applyAction(e, true, Action.APPEND, "2", "a");
        Assert.assertArrayEquals(new String[]{"1", "2"}, (String[])e.get("a"));

        e.put("a", new String[]{"1"});
        applyAction(e, true, Action.APPEND, null, "a");
        Assert.assertArrayEquals(new String[]{"1", null}, (String[])e.get("a"));

        e.put("a", new String[]{"1"});
        applyAction(e, true, Action.APPEND, NullOrMissingValue.NULL, "a");
        Assert.assertArrayEquals(new String[]{"1", null}, (String[])e.get("a"));

        Assert.assertThrows(IgnoredEventException.class,
                () -> applyAction(e, false, Action.APPEND, NullOrMissingValue.MISSING, "a"));

        applyAction(e, true, Action.APPEND, "1", "b");
        Assert.assertEquals(List.of("1"), e.get("b"));

        applyAction(e, true, Action.APPEND, null, "c");
        Assert.assertEquals(List.of(NullOrMissingValue.NULL), e.get("c"));
    }

    private void applyAction(Event e, Object expected, Action a, Object value, String... path) {
        Assert.assertEquals("Didn't resolve the path correctly",  expected, e.applyAtPath(a, VariablePath.of(path), value));
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
        Event wrapped = event.wrap();
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
