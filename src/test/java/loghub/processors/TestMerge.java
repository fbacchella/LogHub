package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;

public class TestMerge {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Merge");
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void test() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", seeds: {\"a\": 0, \"b\": \",\", \"d\": 0.0, \"e\": null, \"c\": [], \"count\": 'c', \"@timestamp\": '>', \"f\": {}}, doFire: [a] >= 2, inPipeline: \"main\", forward: false}}";

        Properties p = Configuration.parse(new StringReader(conf));
        Assert.assertTrue(p.pipelines.stream().allMatch(i-> i.configure(p)));
        Merge m = (Merge) p.namedPipeLine.get("main").processors.stream().findFirst().get();
        try {
            m.process(Event.emptyEvent(ConnectionContext.EMPTY));
        } catch (ProcessorException.DroppedEventException e1) {
        }
        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.setTimestamp(new Date(0));
        e.put("a", 1);
        e.put("b", 2);
        e.put("c", 3);
        e.put("d", 4);
        e.put("e", "5");
        e.put("f", Collections.singletonMap(".f", 1));
        boolean dropped = false;
        try {
            m.process(e);
        } catch (ProcessorException.DroppedEventException e1) {
            dropped = true;
        }
        long timestamp = 0;
        try {
            e.setTimestamp(new Date());
            e.put("f", Collections.singletonMap(".f", "2"));
            timestamp = e.getTimestamp().getTime();
            Assert.assertTrue(m.process(e));
        } catch (ProcessorException.DroppedEventException e1) {
            dropped = true;
        }
        Thread.yield();
        e = p.mainQueue.remove();
        Assert.assertTrue(dropped);
        Assert.assertTrue(p.mainQueue.isEmpty());
        Assert.assertEquals("2,2", e.get("b"));
        Assert.assertEquals(8.0, (double) e.get("d"), 1e-5);
        Assert.assertEquals(null, e.get("e"));
        Assert.assertTrue(e.get("f") instanceof Map);
        Assert.assertTrue(((Map)e.get("f")).get(".f") instanceof List);
        Assert.assertEquals(timestamp, e.getTimestamp().getTime());

    }

    @Test(timeout=5000)
    public void testTimeout() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", seeds: {\"a\": 0, \"b\": \",\", \"e\": 'c', \"c\": [], \"@timestamp\": null}, inPipeline: \"main\", timeout: 1 }}";

        Properties p = Configuration.parse(new StringReader(conf));
        Assert.assertTrue(p.pipelines.stream().allMatch(i-> i.configure(p)));
        Merge m = (Merge) p.namedPipeLine.get("main").processors.stream().findFirst().get();
        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.setTimestamp(new Date(0));
        e.put("a", 1);
        e.put("b", 2);
        e.put("c", 3);
        e.put("d", 4);
        e.put("e", "5");
        try {
            m.process(e);
        } catch (ProcessorException.DroppedEventException e1) {
        }
        Assert.assertTrue(p.mainQueue.isEmpty());
        Thread.sleep(2000);
        e = p.mainQueue.element();
        Assert.assertEquals(String.class, e.get("b").getClass());
        Assert.assertEquals("2", e.get("b"));
        Assert.assertEquals(1L, e.get("e"));
        Assert.assertNotEquals(0L, e.getTimestamp().getTime());
    }

    @Test
    public void testDefault() throws Throwable {
        String conf= "pipeline[main] { merge {index: \"${e%s}\", doFire: true, default: null, onFire: $main, forward: true, inPipeline: \"main\"}}";

        Properties p = Configuration.parse(new StringReader(conf));
        Assert.assertTrue(p.pipelines.stream().allMatch(i-> i.configure(p)));
        Merge m = (Merge) p.namedPipeLine.get("main").processors.stream().findFirst().get();
        Event e = Event.emptyEvent(ConnectionContext.EMPTY);
        e.setTimestamp(new Date(0));
        e.put("a", 1);
        e.put("b", 2);
        e.put("c", 3);
        e.put("d", '4');
        e.put("e", "5");
        boolean dropped = false;
        try {
            m.process(e);
        } catch (ProcessorException.DroppedEventException e1) {
            dropped = true;
        }
        try {
            e.setTimestamp(new Date(1));
            m.process(e);
        } catch (ProcessorException.DroppedEventException e1) {
            dropped = true;
        }
        e = p.mainQueue.remove();
        Assert.assertFalse(dropped);
        Assert.assertFalse(p.mainQueue.isEmpty());
        Assert.assertEquals(0, e.keySet().size());
        Assert.assertEquals(0L, e.getTimestamp().getTime());

    }

}
