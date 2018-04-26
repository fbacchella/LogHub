package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.processors.Identity;
import loghub.processors.SyslogPriority;

public class TestConfigurations {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.Pipeline", "loghub.configuration","loghub.receivers.ZMQ", "loghub.Receiver", "loghub.processors.Forker", "loghub", "loghub.EventsProcessor");
    }

    @Test
    public void testBuildPipeline() throws IOException, InterruptedException, ConfigException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        conf.mainQueue.add(sent);
        Event received = conf.mainQueue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test
    public void testBuildSubPipeline() throws IOException, InterruptedException, ConfigException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = Tools.getEvent();

        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        conf.mainQueue.add(sent);
        Event received = conf.mainQueue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test
    public void testTwoPipe() throws InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("twopipe.conf");
        Thread t = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        t.start();

        Event sent = Tools.getEvent();
        sent.put("message", "1");
        sent.inject(conf.namedPipeLine.get("pipeone"), conf.mainQueue);

        try {
            Event re = conf.outputQueues.get("main").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals("wrong event received", "1", re.get("message"));
        } finally {
            logger.debug("{} is at main pipeline {}", sent, sent.getCurrentPipeline());
            t.interrupt();
        }
    }

    @Test
    public void testFork() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("forkforward.conf");
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ep.start();

        try {
            Event sent = Tools.getEvent();
            sent.inject(conf.namedPipeLine.get("mainfork"), conf.mainQueue);
            Event forked = conf.outputQueues.get("forked").poll(1, TimeUnit.SECONDS);
            Event initial = conf.outputQueues.get("mainfork").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals(1, forked.size());
            Assert.assertEquals(1, initial.size());
            Assert.assertEquals(3, forked.get("b"));
            Assert.assertEquals(1, initial.get("a"));
        } finally {
            ep.interrupt();
        }
    }

    @Test
    public void testForward() throws InterruptedException, ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("forkforward.conf");
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ep.start();

        try {
            Event sent = Tools.getEvent();
            sent.inject(conf.namedPipeLine.get("mainforward"), conf.mainQueue);
            Event forwarded = conf.outputQueues.get("forked").poll(1, TimeUnit.SECONDS);
            Event initial = conf.outputQueues.get("mainforward").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals(1, forwarded.size());
            Assert.assertNull(initial);
            Assert.assertEquals(3, forwarded.get("b"));
            Assert.assertNull(sent.get("a"));
        } finally {
            ep.interrupt();
        }
    }

    @Test
    public void testComplexConf() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("test.conf", false);
        for(String plName: new String[]{"main", "oneref", "groovy"}) {
            Assert.assertTrue("pipeline '" + plName +"'not found", conf.namedPipeLine.containsKey(plName));
        }
        Assert.assertEquals("input not found", 1, conf.receivers.size());
        Assert.assertEquals("ouput not found", 2, conf.senders.size());
    }

    @Test
    public void testArray() throws ConfigException, IOException {
        Tools.loadConf("array.conf", false);
    }

    @Test
    public void testSubPipeline() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("subpipeline.conf");
        Event sent = Tools.getEvent();
        sent.put("a", "1");
        sent.put("b", "2");

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event received = conf.mainQueue.remove();
        Assert.assertEquals("Subpipeline not processed", 1, received.get("a"));
        Assert.assertEquals("Subpipeline not processed", 2, received.get("b"));
    }

    // Ensure that multi-fields processor don't access a success sub-pipeline
    @Test
    public void testBadFields1() {
        SyslogPriority processor = new SyslogPriority();
        processor.setSuccess(new Identity());
        processor.setFields(new String[]{"a", "b"});
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    // Ensure that multi-fields processor don't access a success sub-pipeline
    @Test
    public void testBadFields2() {
        SyslogPriority processor = new SyslogPriority();
        processor.setFailure(new Identity());
        processor.setFields(new String[]{"a", "b"});
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    // Ensure that multi-fields processor don't access a success sub-pipeline
    @Test
    public void testBadFields3() {
        SyslogPriority processor = new SyslogPriority();
        processor.setException(new Identity());
        processor.setFields(new String[]{"a", "b"});
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    @Test
    public void testConfigurationWalkin() throws ConfigException, IOException {
        String confile = "includes: \"" +  Configuration.class.getClassLoader().getResource("includes").getFile() + "/*.conf\"";
        Properties props = Configuration.parse(new StringReader(confile));
        Assert.assertTrue(props.namedPipeLine.containsKey("empty"));
    }

}
