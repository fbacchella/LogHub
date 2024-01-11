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

public class TestFilter {

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.processors.Filter");
    }

    private Event runTest(String conf) throws ProcessorException, IOException {
        Properties p = Configuration.parse(new StringReader(conf));
        Processor pr = p.namedPipeLine.get("main").processors.get(0);
        Assert.assertTrue(pr.configure(p));

        EventsFactory factory = new EventsFactory();
        Event ev = factory.newEvent();
        ev.put("a1", 0);
        ev.putAtPath(VariablePath.of("a", "b"), 0);
        ev.put("a2", 1);
        ev.put("a3", List.of(0));
        ev.put("c", 0);
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        return ev;
    }

    @Test
    public void test() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> x == 0, fields: [\"a*\", \"b*\"],}}";
        Event ev = runTest(conf);
        Assert.assertEquals(2, ev.size());
        Assert.assertEquals(1, ev.get("a2"));
        Assert.assertFalse(ev.containsKey("a3"));
        Assert.assertEquals(0, ev.get("c"));
    }

    @Test
    public void testNoIterate() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> x == 0, fields: [\"a*\", \"b*\"], iterate: false}}";
        Event ev = runTest(conf);
        Assert.assertEquals(3, ev.size());
        Assert.assertEquals(1, ev.get("a2"));
        Assert.assertEquals(List.of(0), ev.get("a3"));
        Assert.assertEquals(0, ev.get("c"));
    }

}
