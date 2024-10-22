package loghub.processors;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import loghub.Helpers;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestMapper {

    private final EventsFactory factory = new EventsFactory();

    @Test
    public void test1() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", 1);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper1"), conf);
        Assert.assertEquals("conversion not expected", "b", sent.get("d"));
    }

    @Test
    public void test2() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", 1);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", "b", sent.get("a"));
    }

    @Test
    public void testNotMapped() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", 3);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", 3, sent.get("a"));
    }

    @Test
    public void test4() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", 2L);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper2"), conf);
        Assert.assertEquals("conversion not expected", "c", sent.get("a"));
    }

    @Test
    public void testExpression() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("map.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", 2);

        Tools.runProcessing(sent, conf.namedPipeLine.get("mapper3"), conf);
        Assert.assertEquals("conversion not expected", "c", sent.get("a"));
    }

}
