package loghub.processors;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.senders.BlockingConnectionContext;

public class TestForEach {

    private static final Logger logger = LogManager.getLogger();
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.processors.ForEach", "loghub.EventsProcessor");
    }

    @Test
    public void simpleTestList() throws IOException, InterruptedException {
        List<Map<String, String>> evData = List.of(new HashMap<>(Map.of("b", "1")), new HashMap<>(Map.of("b", "2")));
        runCommon(evData);
    }

    @Test
    public void simpleTestArray() throws IOException, InterruptedException {
        @SuppressWarnings("unchecked")
        Map<String, String>[] evData = new Map[]{new HashMap<>(Map.of("b", "1")), new HashMap<>(Map.of("b", "2"))};
        runCommon(evData);
    }

    private void runCommon(Object evData) throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a"), evData);
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([c]=[b])}"), populate);
        Assert.assertEquals(1, ev.size());
        @SuppressWarnings("unchecked")
        List<Map<String, String>> content = (List<Map<String, String>>) ev.get("a");
        Assert.assertEquals(2, content.size());
        Assert.assertEquals("1", content.get(0).get("b"));
        Assert.assertEquals("1", content.get(0).get("c"));
        Assert.assertEquals("2", content.get(1).get("b"));
        Assert.assertEquals("2", content.get(1).get("c"));
    }

    @Test
    public void noList() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a", "b"), "1");
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([c]=[b])}"), populate);
        Assert.assertEquals(1, ev.size());
        Assert.assertEquals("1", ev.getAtPath(VariablePath.of("a", "b")));
    }

    @Test
    public void missing() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("b"), "1");
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([c]=[b])}"), populate);
        Assert.assertEquals(1, ev.size());
        Assert.assertEquals("1", ev.getAtPath(VariablePath.of("b")));
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("a")));
    }

    @Test
    public void nullValue() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a"), null);
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([c]=[b])}"), populate);
        Assert.assertEquals(1, ev.size());
        Assert.assertEquals(NullOrMissingValue.NULL, ev.getAtPath(VariablePath.of("a")));
    }

    @Test
    public void copy() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a"), List.of(new HashMap<>(Map.of("b", "1")), new HashMap<>(Map.of("b", "2"))));
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([b] == \"1\" ? [. c]=[^]) }"), populate);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> l = (List<Map<?, ?>>) ev.get("a");
        Assert.assertEquals(1, l.get(0).size());
        Assert.assertEquals(1, l.get(1).size());
        Assert.assertEquals(Map.of("b", "1"), ev.get("c"));
    }

    @Test
    public void move() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a"), List.of(new HashMap<>(Map.of("b", "1")), new HashMap<>(Map.of("b", "2"))));
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([b] == \"1\" ? [. c] < [^]) }"), populate);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> l = (List<Map<?, ?>>) ev.get("a");
        Assert.assertEquals(0, l.get(0).size());
        Assert.assertEquals(1, l.get(1).size());
        Assert.assertEquals(Map.of("b", "1"), ev.get("c"));
    }

    @Test
    public void delete() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(VariablePath.of("a"), List.of(new HashMap<>(Map.of("b", "1")), new HashMap<>(Map.of("b", "2"))));
        Event ev = run(new StringReader("pipeline[main]{ foreach[a]([b] == \"1\" ? [^]-) }"), populate);
        @SuppressWarnings("unchecked")
        List<Map<?, ?>> l = (List<Map<?, ?>>) ev.get("a");
        Assert.assertTrue(l.get(0).isEmpty());
        Assert.assertEquals(Map.of("b", "2"), l.get(1));
    }

    public Event run(Reader r, Consumer<Event> populate) throws IOException, InterruptedException {
        BlockingConnectionContext ctx = new BlockingConnectionContext();
        Properties props = Tools.loadConf(r);
        EventsProcessor ep = new EventsProcessor(props.mainQueue, props.outputQueues, props.namedPipeLine, 100, props.repository);
        ep.start();
        Event ev = factory.newEvent(ctx);
        populate.accept(ev);
        ev.inject(props.namedPipeLine.get("main"), props.mainQueue, true);
        boolean computed = ctx.getLocalAddress().tryAcquire(5, TimeUnit.SECONDS);
        Assert.assertTrue(computed);
        ep.interrupt();
        return ev;
    }

}
