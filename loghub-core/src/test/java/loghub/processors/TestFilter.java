package loghub.processors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestFilter {

    private static final Logger logger = LogManager.getLogger();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.processors.Filter");
    }

    private Event runTest(String conf, Consumer<Event> fillEvent) throws ProcessorException, IOException {
        Properties p = Configuration.parse(new StringReader(conf));
        Processor pr = p.namedPipeLine.get("main").processors.get(0);
        Assert.assertTrue(pr.configure(p));

        EventsFactory factory = new EventsFactory();
        Event ev = factory.newEvent();
        fillEvent.accept(ev);
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        logger.debug("Processed as {}", () -> ev);
        return ev;
    }

    private void commonFill(Event ev) {
        ev.put("a1", 0);
        ev.putAtPath(VariablePath.of("a2", "b"), 0);
        ev.putAtPath(VariablePath.of("a2", "c"), List.of());
        ev.put("a3", 1);
        ev.put("a4", List.of(0));
        ev.put("a5", List.of());
        ev.putAtPath(VariablePath.of("a6", "b"), List.of());
        ev.put("c", 0);
        ev.put("d", List.of());
    }

    @Test
    public void testZero() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> x == 0, fields: [\"a*\", \"b*\"],}}";
        Event ev = runTest(conf, this::commonFill);
        Assert.assertEquals(7, ev.size());
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("a2", "b")));
        Assert.assertEquals(List.of(), ev.getAtPath(VariablePath.of("a2", "c")));
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a3")));
        Assert.assertEquals(List.of(), ev.getAtPath(VariablePath.of("a5")));
        Assert.assertEquals(0, ev.get("c"));
        Assert.assertEquals(List.of(), ev.get("d"));
    }

    @Test
    public void testEmpty() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> isEmpty(x), field: [a2],} | loghub.processors.Filter {lambda: x -> isEmpty(x), field: [a6],}}";
        Event ev = runTest(conf, this::commonFill);
        Assert.assertEquals(7, ev.size());
        Assert.assertEquals(0, ev.getAtPath(VariablePath.of("a2", "b")));
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("a2", "c")));
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("a6")));
        Assert.assertEquals(1, ev.get("a3"));
        Assert.assertEquals(0, ev.get("c"));
    }

    @Test
    public void testRelated() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> isEmpty(x), field: [related],}}";
        Event ev = runTest(conf, e -> {
            e.putAtPath(VariablePath.of("related", "hosts"), Set.of());
            e.putAtPath(VariablePath.of("related", "ip"), Set.of());
            e.putAtPath(VariablePath.of("related", "user"), Set.of("root", "apache"));
            e.putAtPath(VariablePath.of("related", "hash"), Set.of());
        });
        Assert.assertEquals(1, ev.size());
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("related", "hosts")));
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("related", "ip")));
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("related", "hash")));
        Assert.assertEquals(Set.of("apache", "root"), Set.copyOf((Collection<?>) ev.getAtPath(VariablePath.of("related", "user"))));
    }

    @Test
    public void testNoIterate() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> x == 0, fields: [\"a*\", \"b*\"], iterate: false}}";
        Event ev = runTest(conf, this::commonFill);
        Assert.assertEquals(7, ev.size());
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getAtPath(VariablePath.of("a2", "b")));
        Assert.assertEquals(List.of(0), ev.get("a4"));
        Assert.assertEquals(0, ev.get("c"));
    }

}
