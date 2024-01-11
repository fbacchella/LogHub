package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestMap {

    final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.processors.Filter");
    }

    public void runTest(String conf, Event ev) throws IOException, ProcessorException {
        Properties p = Configuration.parse(new StringReader(conf));
        Processor pr = p.namedPipeLine.get("main").processors.get(0);
        Assert.assertTrue(pr.configure(p));
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
    }

    @Test
    public void test() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Map {lambda: x -> x + 1, fields: [\"a*\", \"b*\"],}}";
        Event ev = factory.newEvent();
        ev.put("a1", 0);
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        ev.put("a2", 2);
        ev.put("c", 0);
        runTest(conf, ev);
        Assert.assertEquals(4, ev.size());
        Assert.assertEquals(0, ev.get("c"));
        Assert.assertEquals(1, ev.get("a1"));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("a", "b")));
        Assert.assertEquals(3, ev.get("a2"));
    }

    @Test
    public void testIterable() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Map {lambda: x -> x + 1, field: [a],}}";
        Event ev = factory.newEvent();
        ev.put("a", new Object[]{1, 2, 3});
        runTest(conf, ev);
        Assert.assertEquals(1, ev.size());
        @SuppressWarnings("unchecked")
        List<Object> a = (List<Object>) ev.get("a");
        Assert.assertEquals(List.of(2, 3, 4), a);
    }

    @Test
    public void testNotIterate() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Map {lambda: x -> x + 1, field: [a], iterate: false}}";
        Event ev = factory.newEvent();
        ev.put("a", new Object[]{1, 2, 3});
        runTest(conf, ev);
        Assert.assertEquals(1, ev.size());
        Object[] a = (Object[]) ev.get("a");
        Assert.assertArrayEquals(new Object[]{1, 2, 3, 1}, a);
    }

}
