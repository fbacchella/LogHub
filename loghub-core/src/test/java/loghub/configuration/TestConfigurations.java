package loghub.configuration;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import loghub.BuilderClass;
import loghub.EventsProcessor;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.ZMQFactory;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Identity;
import loghub.processors.SyslogPriority;
import loghub.processors.UrlParser;
import lombok.Getter;
import lombok.Setter;

public class TestConfigurations {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @Rule
    public ZMQFactory tctxt = new ZMQFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.Pipeline", "loghub.configuration", "loghub.receivers.ZMQ", "loghub.Receiver", "loghub.processors.Forker", "loghub", "loghub.EventsProcessor");
    }

    @Test
    public void testBuildPipeline() throws IOException, InterruptedException, ConfigException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = factory.newEvent();
        logger.debug("pipelines: " + conf.pipelines);
        logger.debug("namedPipeLine: " + conf.namedPipeLine);
        conf.mainQueue.add(sent);
        Event received = conf.mainQueue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals("not expected event received", sent, received);
    }

    @Test
    public void testBuildSubPipeline() throws IOException, InterruptedException, ConfigException {
        Properties conf = Tools.loadConf("simple.conf");
        Event sent = factory.newEvent();

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

        Event sent = factory.newEvent();
        sent.put("message", "1");
        sent.inject(conf.namedPipeLine.get("pipeone"), conf.mainQueue, true);

        try {
            Event re = conf.outputQueues.get("main").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals("wrong event received", "1", re.get("message"));
        } finally {
            logger.debug("{} is at main pipeline {}", sent, sent.getCurrentPipeline());
            t.interrupt();
        }
    }

    @Test
    public void testFork() throws InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("forkforward.conf");
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ep.start();

        try {
            Event sent = factory.newEvent();
            sent.inject(conf.namedPipeLine.get("mainfork"), conf.mainQueue, true);
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
    public void testForward() throws InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("forkforward.conf");
        EventsProcessor ep = new EventsProcessor(conf.mainQueue, conf.outputQueues, conf.namedPipeLine, conf.maxSteps, conf.repository);
        ep.start();

        try {
            Event sent = factory.newEvent();
            sent.inject(conf.namedPipeLine.get("mainforward"), conf.mainQueue, true);
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
        for (String plName : new String[]{"main", "oneref", "groovy"}) {
            Assert.assertTrue("pipeline '" + plName + "'not found", conf.namedPipeLine.containsKey(plName));
        }
        Assert.assertEquals("input not found", 1, conf.receivers.size());
        Assert.assertEquals("ouput not found", 3, conf.senders.size());
        Assert.assertEquals(5, conf.mainQueue.remainingCapacity());
        Assert.assertEquals(5, conf.mainQueue.remainingBlockingCapacity());
        Assert.assertEquals(10, conf.mainQueue.getWeight());
    }

    @Getter
    @BuilderClass(TestArrayProcessor.Builder.class)
    public static class TestArrayProcessor extends Processor {
        @Setter
        public static class Builder extends Processor.Builder<TestArrayProcessor> {
            private Expression[] expressions;
            public TestArrayProcessor build() {
                return new TestArrayProcessor(this);
            }
        }
        public static TestArrayProcessor.Builder getBuilder() {
            return new TestArrayProcessor.Builder();
        }

        private final Expression[] expressions;
        public TestArrayProcessor(Builder builder) {
            expressions = builder.expressions;
        }

        @Override
        public boolean process(Event event) {
            return false;
        }
    }

    @Test
    public void testArray() throws ConfigException, IOException, ProcessorException {
        Properties p = Tools.loadConf("array.conf", false);
        List<Processor> main = p.namedPipeLine.get("main").processors;
        SyslogPriority pr = (SyslogPriority) main.get(0);
        String[] fields = pr.getFields();
        Assert.assertEquals(2, fields.length);
        Assert.assertEquals("a", fields[0]);
        Assert.assertEquals("b", fields[1]);
        TestArrayProcessor pr2 = (TestArrayProcessor) main.get(1);
        Expression[] expressions = pr2.getExpressions();
        Assert.assertEquals(2, expressions.length);
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", 2);
        Assert.assertEquals(1, expressions[0].eval(ev));
        Assert.assertEquals(2, expressions[1].eval(ev));
    }

    @Test
    public void testLog() throws ConfigException, IOException {
        String confile = "pipeline[main] {log([info], INFO)}";
        Properties conf = Configuration.parse(new StringReader(confile));
        Event sent = factory.newEvent();
        sent.put("a", "1");
        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);
        Event received = conf.mainQueue.remove();
        Assert.assertEquals("Subpipeline not processed", "1", received.get("a"));
    }

    @Test
    public void testSubPipeline() throws ConfigException, IOException {
        Properties conf = Tools.loadConf("subpipeline.conf");
        Event sent = factory.newEvent();
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
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setSuccess(new Identity());
        builder.setFields(new String[]{"a", "b"});
        SyslogPriority processor = builder.build();
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    // Ensure that multi-fields processor don't access a success sub-pipeline
    @Test
    public void testBadFields2() {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setFailure(new Identity());
        builder.setFields(new String[]{"a", "b"});
        SyslogPriority processor = builder.build();
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    // Ensure that multi-fields processor don't access a success sub-pipeline
    @Test
    public void testBadFields3() {
        SyslogPriority.Builder builder = SyslogPriority.getBuilder();
        builder.setException(new Identity());
        builder.setFields(new String[]{"a", "b"});
        SyslogPriority processor = builder.build();
        Assert.assertFalse(processor.configure(new Properties(Collections.emptyMap())));
    }

    @Test
    public void testConfigurationWalking() throws ConfigException, IOException {
        String confile = "includes: \"" +  Configuration.class.getClassLoader().getResource("includes").getFile() + "/*.conf\"";
        Properties props = Configuration.parse(new StringReader(confile));
        Assert.assertTrue(props.namedPipeLine.containsKey("empty"));
    }

    @Test(expected = ConfigException.class)
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
    public void testMissingPipeInput() throws ConfigException {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> {
            String confile = "input { loghub.receivers.TimeSerie } | $pipe";
            Configuration.parse(new StringReader(confile));
        });
        Assert.assertTrue(ex.getMessage().startsWith("Invalid input, no destination pipeline"));
    }

    @Test
    public void testBadBean() throws ConfigException {
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> {
            String confile = "pipeline[bad] {\n" +
                    "   loghub.processors.Identity {bad: true}\n" +
                    "}\n";
            Configuration.parse(new StringReader(confile));
        });
        Assert.assertEquals("Unknown bean 'bad' for loghub.processors.Identity", ex.getMessage());
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

        Event sent = factory.newEvent();
        sent.put("b", "a\nb1");
        sent.inject(conf.namedPipeLine.get("pattern"), conf.mainQueue, true);
        try {
            sent.inject(conf.namedPipeLine.get("pattern"), conf.mainQueue, true);
            Event received = conf.outputQueues.get("pattern").poll(1, TimeUnit.SECONDS);
            Assert.assertEquals(1, received.get("a"));
        } finally {
            ep.interrupt();
        }
    }

    @Test
    public void testPathString() throws ConfigException, IOException {
        String confile = "pipeline[withpath] {\n" +
                        "   loghub.processors.UrlParser {field: \"a\", path: \"b\"}\n" +
                        "}\n";
        Properties  p = Configuration.parse(new StringReader(confile));
        UrlParser pr = (UrlParser) p.namedPipeLine.get("withpath").processors.get(0);
        Assert.assertEquals("a", pr.getField().get(0));
        Assert.assertEquals("b", pr.getPathArray().get(0));
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.parse("b.a"), "http://loghub.fr/?test=true");
        Tools.runProcessing(ev, p.namedPipeLine.get("withpath"), p);
        Assert.assertEquals("/", ev.getAtPath(VariablePath.parse("b.a.path")));
        Assert.assertEquals("http", ev.getAtPath(VariablePath.parse("b.a.scheme")));
        Assert.assertEquals("loghub.fr", ev.getAtPath(VariablePath.parse("b.a.domain")));
        Assert.assertEquals("test=true", ev.getAtPath(VariablePath.parse("b.a.query")));
    }

    @Test
    public void testPathEventVariable() throws ConfigException, IOException {
        String confile = "pipeline[withpath] {\n" +
                        "   loghub.processors.UrlParser {field: [a b], path: [c d]}\n" +
                        "}\n";
        Properties  p =  Configuration.parse(new StringReader(confile));
        UrlParser pr = (UrlParser) p.namedPipeLine.get("withpath").processors.get(0);
        Assert.assertEquals("a", pr.getField().get(0));
        Assert.assertEquals("b", pr.getField().get(1));
        Assert.assertEquals("c", pr.getPathArray().get(0));
        Assert.assertEquals("d", pr.getPathArray().get(1));
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.parse("c.d.a.b"), "http://loghub.fr/?test=true");
        Tools.runProcessing(ev, p.namedPipeLine.get("withpath"), p);
        Assert.assertEquals("/", ev.getAtPath(VariablePath.parse("c.d.a.b.path")));
        Assert.assertEquals("http", ev.getAtPath(VariablePath.parse("c.d.a.b.scheme")));
        Assert.assertEquals("loghub.fr", ev.getAtPath(VariablePath.parse("c.d.a.b.domain")));
        Assert.assertEquals("test=true", ev.getAtPath(VariablePath.parse("c.d.a.b.query")));
    }

    @Test
    public void testBadCharset() throws ConfigException, IOException {
        try {
            String confile = "input {\n" +
                            "    loghub.receivers.TimeSerie { decoder: loghub.decoders.StringCodec { charset: \"NONE\"}, frequency: 10 }\n" +
                            "} | $main\n";
            Configuration.parse(new StringReader(confile));
            Assert.fail("An exception was expected");
        } catch (ConfigException e) {
            Assert.assertEquals("Unsupported charset name: NONE", e.getMessage());
            Assert.assertEquals("file <unknown>, line 2:42", e.getLocation());
        }
    }

    @Test
    public void testInclude() throws ConfigException, IOException {
        Path confincludes = Paths.get(TestConfigurations.class.getClassLoader().getResource("includes").getFile());
        Path relativePath = Paths.get(".").toAbsolutePath().normalize().relativize(confincludes.normalize().toAbsolutePath());
        for (String confile : new String[] {
                String.format("includes: \"%s/?.conf\"", relativePath),
                String.format("includes: [\"%s/?.conf\"]", relativePath),
                String.format("includes: \"%s/?.conf\"", confincludes),
                String.format("includes: \"%s/recurse.conf\"", confincludes),
                String.format("includes: \"%s\"", confincludes),
                String.format("includes: \"%s\"", relativePath),
                String.format("includes: [\"%s/a.conf\", \"%s/b.conf\"]", relativePath, confincludes),
        }) {
            logger.info("trying {}", confile);
            Properties p =  Configuration.parse(new StringReader(confile));
            Assert.assertTrue((Boolean) p.get("a"));
            Assert.assertTrue((Boolean) p.get("b"));
        }

        ConfigException failed = Assert.assertThrows(ConfigException.class, () -> Configuration.parse(new StringReader(String.format("includes: \"%s/none.conf\"", relativePath))));
        Assert.assertEquals("No Configuration files found", failed.getMessage());
    }

    @Test
    public void testBadProperty() {
        for (String confile : List.of(
                "queueDepth: 'a'",
                "http.jwt: \"true\"",
                "timezone: 1",
                "locale: true",
                "encoder: loghub.encoders.ToJson")) {
            ConfigException ex = Assert.assertThrows(ConfigException.class, () -> Configuration.parse(new StringReader(confile)));
        }
    }

    @Test
    public void testGoodProperty() throws IOException {
        Map<String, Object> properties = Map.of(
                "a: 0x0a", 10,
                "a: 0o12", 10,
                "a: 0b1010", 10,
                "a: true", true,
                "a: 1.0", 1.0,
                "a: [\"a\", \"b\"]", new String[]{"a", "b"});
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            Properties props = Configuration.parse(new StringReader(property.getKey()));
            String message = "For " + property.getKey();
            if (property.getValue().getClass().isArray()) {
                Assert.assertArrayEquals(message, (Object[]) property.getValue(), (Object[]) props.get("a"));
            } else {
                Assert.assertEquals(message, property.getValue(), props.get("a"));
            }
        }
    }

    @Test
    public void testin() throws ConfigException, IOException {
        String confile = "pipeline[main] {\n" +
                                 "[a b] in list(\"1\") ? [b c] =+ 1 |\n" +
                                 "[a b] in list(\"1\") ? [b c] =+ 2 " +
                                 "}\n";
        Properties  p = Configuration.parse(new StringReader(confile));
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), "1");
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assert.assertEquals(List.of(1, 2), ev.getAtPath(VariablePath.of("b", "c")));
    }

    @Test
    public void testSet() throws ConfigException, IOException {
        String confile = "pipeline[main] {\n" +
                                 "[related ip] = set() |" +
                                 "[related ip] =+ \"127.0.0.1\" |" +
                                 "[related ip] =+ \"127.0.0.1\" |" +
                                 "[related ip] =+ \"127.0.0.2\" |" +
                                 "    loghub.processors.Map { field: [related ip], lambda: x -> (java.net.InetAddress) x,  }\n" +
                                 "}\n";
        Properties  p = Configuration.parse(new StringReader(confile));
        Event ev = factory.newEvent();
        Tools.runProcessing(ev, p.namedPipeLine.get("main"), p);
        Assert.assertEquals(Set.of(InetAddress.getByName("127.0.0.1"), InetAddress.getByName("127.0.0.2")), ev.getAtPath(VariablePath.of("related", "ip")));
    }

}
