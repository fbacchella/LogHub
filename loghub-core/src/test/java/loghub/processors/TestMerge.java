package loghub.processors;

import java.io.StringReader;
import java.util.ArrayList;
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

import loghub.AsyncProcessor;
import loghub.DiscardedEventException;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestMerge {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Merge", "loghub.EventsRepository");
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", seeds: {\"a\": 0, \"b\": \",\", \"d\": 0.0, \"e\": null, \"c\": [], \"count\": 'c', \"@timestamp\": '>', \"f\": {}}, doFire: [a] >= 2, forward: false}}";

        Properties p = Configuration.parse(new StringReader(conf));
        Helpers.parallelStartProcessor(p);
        Merge m = (Merge) p.namedPipeLine.get("main").processors.stream().findFirst().get();

        List<Event> events = new ArrayList<>();
        Assert.assertFalse(m.process(factory.newEvent()));

        Event e1 = factory.newEvent();
        e1.setTimestamp(new Date(0));
        e1.put("a", 1);
        e1.put("b", 2);
        e1.put("c", 3);
        e1.put("d", 4);
        e1.put("e", "5");
        Map<String, Object> f1 = new HashMap<>(1);
        f1.put(".f", this);
        e1.put("f", f1);
        e1.putMeta("g", 7);

        Event e2 = factory.newEvent();
        e2.putAll(e1);
        e2.setTimestamp(new Date(1));
        Map<String, Object> f2 = new HashMap<>(1);
        f2.put(".f", this);
        e2.put("f", f2);
        e2.putMeta("g", "8");

        events.add(e1);
        events.add(e2);

        AsyncProcessor.PausedEventException ex = Assert.assertThrows(AsyncProcessor.PausedEventException.class, () -> m.process(e1));
        Assert.assertNull(ex.getFuture());
        Assert.assertThrows(DiscardedEventException.class, () -> m.process(e2));

        Thread.yield();
        Event e = p.mainQueue.remove();
        Assert.assertTrue(p.mainQueue.isEmpty());
        Assert.assertEquals("2,2", e.get("b"));
        Assert.assertEquals(8.0, (double) e.get("d"), 1e-5);
        Assert.assertEquals("5", e.get("e"));
        Assert.assertTrue(e.get("f") instanceof Map);
        Assert.assertTrue(((Map)e.get("f")).get(".f") instanceof List);
        Assert.assertEquals(1, e.getTimestamp().getTime());
        Assert.assertEquals(7, e.getMeta("g"));

    }

    @Test(timeout=5000)
    public void testExpiration() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", seeds: {\"a\": 0, \"b\": \",\", \"e\": 'c', \"c\": [], \"@timestamp\": null}, expiration: 1 }}";

        Properties p = Configuration.parse(new StringReader(conf));
        Helpers.parallelStartProcessor(p);
        Merge m = (Merge) p.namedPipeLine.get("main").processors.get(0);
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.put("a", 1);
        e.put("b", 2);
        e.put("c", 3);
        e.put("d", 4);
        e.put("e", "5");
        AsyncProcessor.PausedEventException ex = Assert.assertThrows(AsyncProcessor.PausedEventException.class, () -> m.process(e));
        Assert.assertNull(ex.getFuture());
        Thread.sleep(2000);
        // Will throw exception if event was not fired
        p.mainQueue.element();
    }

    @Test
    public void testDefault() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", doFire: true, default: \",\", onFire: [f] = 1, forward: true}}";

        Properties p = Configuration.parse(new StringReader(conf));
        Helpers.parallelStartProcessor(p);
        Merge m = (Merge) p.namedPipeLine.get("main").processors.stream().findFirst().get();
        Event e = factory.newEvent();
        e.setTimestamp(new Date(0));
        e.put("a", 1);
        e.put("b", 2);
        e.put("c", 3);
        e.put("d", '4');
        e.put("e", "5");
        AsyncProcessor.PausedEventException ex = Assert.assertThrows(AsyncProcessor.PausedEventException.class, () -> m.process(e));
        Assert.assertNull(ex.getFuture());
        Event e2 = factory.newEvent();
        e2.put("g", 1);
        e2.put("e", "5");
        e2.setTimestamp(new Date(3));
        m.process(e2);
        Thread.sleep(2000);
        Assert.assertNotNull("No event was received", e);
        Assert.assertFalse(p.mainQueue.isEmpty());
        Assert.assertEquals(6, e.keySet().size());
        Assert.assertEquals("5,5", e.get("e"));
        Assert.assertEquals(0L, e.getTimestamp().getTime());
    }

}
