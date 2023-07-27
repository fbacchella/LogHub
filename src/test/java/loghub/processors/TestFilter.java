package loghub.processors;

import java.io.IOException;
import java.io.StringReader;

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

    @Test
    public void parse() throws IOException, ProcessorException {
        String conf = "pipeline[main]{ loghub.processors.Filter {lambda: x -> x == 0, fields: [\"a*\", \"b*\"],}}";
        Properties p = Configuration.parse(new StringReader(conf));
        Processor pr = p.namedPipeLine.get("main").processors.get(0);
        Assert.assertTrue(pr.configure(p));

        EventsFactory factory = new EventsFactory();
        Event ev = factory.newEvent();
        ev.put("a1", 0);
        ev.putAtPath(VariablePath.of("a", "b"), 0);
        ev.put("a2", 1);
        ev.put("c", 0);
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assert.assertEquals(2, ev.size());
        Assert.assertEquals(1, ev.get("a2"));
        Assert.assertEquals(0, ev.get("c"));
    }

}
