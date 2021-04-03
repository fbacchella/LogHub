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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import loghub.ZMQFactory;
import loghub.Event;
import loghub.EventsProcessor;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.processors.DecodeUrl;
import loghub.processors.Identity;
import loghub.processors.SyslogPriority;

public class TestConfigurations {

    private static Logger logger;

    @Rule
    public ZMQFactory tctxt = new ZMQFactory();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        Assert.assertEquals(5, conf.mainQueue.remainingCapacity());
        Assert.assertEquals(5, conf.mainQueue.remainingBlockingCapacity());
        Assert.assertEquals(10, conf.mainQueue.getWeight());
    }

    @Test
    public void testArray() throws ConfigException, IOException {
        Properties p = Tools.loadConf("array.conf", false);
        SyslogPriority pr = (SyslogPriority) p.identifiedProcessors.get("withArray");
        String[] fields = (String[]) pr.getFields();
        Assert.assertEquals(1, fields.length);
        Assert.assertEquals("*", fields[0]);
    }

    @Test
    public void testLog() throws ConfigException, IOException, ProcessorException {
        String confile = "pipeline[main] {log([info], INFO)}";
        Properties conf = Configuration.parse(new StringReader(confile));
        Event sent = Tools.getEvent();
        sent.put("a", "1");
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event received = conf.mainQueue.remove();
        Assert.assertEquals("Subpipeline not processed", "1", received.get("a"));
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
    public void testConfigurationWalking() throws ConfigException, IOException {
        String confile = "includes: \"" +  Configuration.class.getClassLoader().getResource("includes").getFile() + "/*.conf\"";
        Properties props = Configuration.parse(new StringReader(confile));
        Assert.assertTrue(props.namedPipeLine.containsKey("empty"));
    }

    @Test(expected=ConfigException.class)
    public void testPipelineReuse() throws ConfigException, IOException {
        String confile = "output $a | {\n" + 
                        "    loghub.senders.Stdout { encoder: loghub.encoders.ToJson}\n" + 
                        "}\n" + 
                        "output $a | {\n" + 
                        "    loghub.senders.Stdout { encoder: loghub.encoders.ToJson}\n" + 
                        "}";
        Configuration.parse(new StringReader(confile));
    }

    @Test
    public void testMissingPipeInput() throws ConfigException, IOException {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("Invalid input, no destination pipeline");

        String confile = "input { loghub.receivers.TimeSerie } | $pipe";
        Configuration.parse(new StringReader(confile));
    }

    @Test
    public void testBadBean() throws ConfigException, IOException {
        thrown.expect(ConfigException.class);
        thrown.expectMessage("Unknown bean 'bad' for loghub.processors.Identity");

        String confile = "pipeline[bad] {\n" + 
                        "   loghub.processors.Identity {bad: true}\n" + 
                        "}\n" + 
                        "";
        Configuration.parse(new StringReader(confile));
    }

    @Test
    public void testComplexPattern() throws ConfigException, IOException, InterruptedException {
        String confile = "pipeline[pattern] {\n" + 
                        "   [b] ==~ /a\\\n" + 
                        "b\\d+/ ? [a]=1:[a]=2" + 
                        "}\n" + 
                        "output $pattern | { loghub.senders.InMemorySender }";

        Properties conf = Tools.loadConf(new StringReader(confile));
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ep.start();

        Event sent = Tools.getEvent();
        sent.put("b", "a\nb1");
        sent.inject(conf.namedPipeLine.get("pattern"), conf.mainQueue);
        try {
            sent.inject(conf.namedPipeLine.get("pattern"), conf.mainQueue);
            Event received = conf.outputQueues.get("pattern").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals(1, received.get("a"));
        } finally {
            ep.interrupt();
        }
    }

    @Test
    public void testPathString() throws ConfigException, IOException {
        String confile = "pipeline[field] {\n" + 
                        "   loghub.processors.DecodeUrl {field: \"a\"}\n" + 
                        "}\n" + 
                        "";
        Properties  p = Configuration.parse(new StringReader(confile));
        DecodeUrl pr = (DecodeUrl) p.namedPipeLine.get("field").processors.get(0);
        Assert.assertEquals("a", pr.getField().get(0));
    }

    @Test
    public void testPathEventVariable() throws ConfigException, IOException {
        String confile = "pipeline[fields] {\n" + 
                        "   loghub.processors.DecodeUrl {field: [a b]}\n" + 
                        "}\n" + 
                        "";
        Properties  p =  Configuration.parse(new StringReader(confile));
        DecodeUrl pr = (DecodeUrl) p.namedPipeLine.get("fields").processors.get(0);
        Assert.assertEquals("a", pr.getField().get(0));
        Assert.assertEquals("b", pr.getField().get(1));
    }

    @Test
    public void testBadCharset() throws ConfigException, IOException {
        try {
            String confile = "input {\n" + 
                            "    loghub.receivers.TimeSerie { decoder: loghub.decoders.StringCodec { charset: \"NONE\"}, frequency: 10 }\n" + 
                            "} | $main\n" + 
                            "";
            Configuration.parse(new StringReader(confile));
            Assert.fail("An exception was expected");
        } catch (ConfigException e) {
            Assert.assertEquals("Unsupported charset name: NONE", e.getMessage());
            Assert.assertEquals("file <unknown>, line 2:42", e.getLocation());
        }
    }

}
