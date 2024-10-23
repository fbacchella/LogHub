package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.EventsProcessor;
import loghub.IgnoredEventException;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Tools;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestWrapping {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Event", "loghub.EventsProcessor", "loghub");
    }

    @Test
    public void testSourceLoadingContains() throws ConfigException, IOException, InterruptedException {
        Event ev = factory.newEvent();
        ev.put("a", new HashMap<String, Object>());
        checkEvent(ev);
    }

    @Test
    public void testSourceLoadingNotContains() throws ConfigException, IOException, InterruptedException {
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

    @Test
    public void testWrongMapping() {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a"), 1);
        Event wrapped = ev.wrap(VariablePath.of("b"));
        Assert.assertTrue(wrapped.keySet().isEmpty());
        Assert.assertTrue(wrapped.entrySet().isEmpty());
        Assert.assertThrows(IgnoredEventException.class, wrapped::size);
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("b", "b")));
        Assert.assertEquals(NullOrMissingValue.MISSING, wrapped.get("b"));
        Assert.assertThrows(IgnoredEventException.class, () -> wrapped.getAtPath(VariablePath.of("^")));
    }

    @Test
    public void fromConfiguration1() throws IOException {
        runEmptyPath("pipeline[main]{path[a]([. c] = [b])}");
    }

    @Test
    public void fromConfiguration2() throws IOException {
        runEmptyPath("pipeline[main]{path[a](isEmpty([b]) ? [. a1] = true) | isEmpty([a b]) ? [. a2] = true}");
    }

    @Test
    public void fromConfiguration3() throws IOException {
        runEmptyPath("pipeline[main]{path[a](isEmpty([^]) ? [. a1] = true) | isEmpty([a b]) ? [. a2] = true}");
    }

    @Test
    public void fromConfiguration4() throws IOException {
        runEmptyPath("pipeline[main]{path[a]([. b] = [^]) | isEmpty([b]) ? [. ab] = true}");
    }

    @Test
    public void canFill() throws IOException {
        Properties p =  Configuration.parse(new StringReader("pipeline[main]{path[a]([. b] = 1 | [b] = 2)}"));
        Event ev = factory.newEvent();
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("a", "b")));
    }

    private void runEmptyPath(String configuration) throws IOException {
        Properties p =  Configuration.parse(new StringReader(configuration));
        Event ev = factory.newEvent();
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assert.assertTrue(ev.isEmpty());
    }

}
