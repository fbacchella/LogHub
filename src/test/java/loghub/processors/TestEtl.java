package loghub.processors;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.LogUtils;
import loghub.Pipeline;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;

public class TestEtl {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Etl", "loghub.EventsProcessor");
    }

    private Event RunEtl(String exp, Consumer<Event> filer) throws ProcessorException {
        Map<String, String> formatters = new HashMap<>();
        Etl e =  ConfigurationTools.buildFromFragment(exp, i -> i.etl(), formatters);
        Map<String, Object> settings = new HashMap<>(1);
        settings.put("__FORMATTERS", formatters);
        e.configure(new Properties(settings));
        Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
        filer.accept(ev);
        Assert.assertTrue(e.process(ev));
        return ev;
    }

    private Etl parseEtl(String exp) {
        Etl e =  ConfigurationTools.buildFromFragment(exp, i -> i.etl());
        e.configure(new Properties(Collections.emptyMap()));
        return e;
    }

    @Test
    public void test1() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue(new String[]{"a", "b"});
        etl.setExpression("event.c + 1");
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("c", 0);
        event.process(etl);
        Assert.assertEquals("evaluation failed", 1, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a", "b"}, null, false));
    }

    @Test
    public void test2() throws ProcessorException {
        Etl etl = new Etl.Remove();
        etl.setLvalue(new String[]{"a"});
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", null, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"a"}, null, false));
    }

    @Test
    public void test3() throws ProcessorException {
        Etl.Rename etl = new Etl.Rename();
        etl.setLvalue(new String[]{"b"});
        etl.setSource(new String[]{"a"});
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 0, event.applyAtPath((i,j,k) -> i.get(j), new String[] {"b"}, null, false));
    }

    @Test
    public void test4() throws ProcessorException {
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue(new String[]{"a"});
        etl.setExpression("formatters.a.format(event.getTimestamp())");
        Map<String, String> formats = Collections.singletonMap("a", "${%t<GMT>H}");
        Map<String, Object> properties = new HashMap<>();
        properties.put("__FORMATTERS", formats);
        boolean done = etl.configure(new Properties(properties));
        Assert.assertTrue("configuration failed", done);
        Event event = Tools.getEvent();
        event.setTimestamp(new Date(3600 * 1000));
        event.process(etl);
        Assert.assertEquals("evaluation failed", "01", event.get("a"));
    }

    @Test
    public void test5() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.put("a", "a");

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);

        Assert.assertEquals("conversion not expected", "a", sent.get("a"));
    }

    @Test
    public void test7() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        Map<String, Object> b = new HashMap<>(1);
        b.put("c", 1);
        sent.put("b", b);

        Tools.runProcessing(sent, conf.namedPipeLine.get("third"), conf);
        Assert.assertEquals("conversion not expected", 1, sent.get("a"));
    }

    @Test
    public void test8() throws ProcessorException, InterruptedException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        for(Pipeline pipe: conf.pipelines) {
            Assert.assertTrue("configuration failed", pipe.configure(conf));
        }
        Event sent = Tools.getEvent();
        sent.setTimestamp(new Date(1));
        Tools.runProcessing(sent, conf.namedPipeLine.get("timestamp"), conf);
        Assert.assertEquals(new Date(0), sent.getTimestamp());
        Assert.assertEquals(new Date(1), sent.get("reception_time"));
    }

    @Test
    public void testAssign() throws ProcessorException {
        Etl e = parseEtl("[a] = 1");
        Event ev = Event.emptyEvent(ConnectionContext.EMPTY);
        Assert.assertTrue(e.process(ev));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testRename() throws ProcessorException {
        Event ev =  RunEtl("[a] < [b]", i -> i.put("b", 1));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testRemove() throws ProcessorException {
        Event ev =  RunEtl("[a]-", i -> i.put("a", 1));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testMap() throws ProcessorException {
        Event ev =  RunEtl("[a] @ [b] { 1: 10, 2: 20 }", i -> i.put("b", 1));
        Assert.assertEquals(10, ev.remove("a"));
        Assert.assertEquals(1, ev.remove("b"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testTimestamp() throws ProcessorException {
        Event ev =  RunEtl("[@timestamp] = 1000", i -> {});
        Assert.assertEquals(1000, ev.getTimestamp().getTime());
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testTimestampMove() throws ProcessorException {
        Event ev =  RunEtl("[@timestamp] < [b]", i -> i.put("b", 1000));
        Assert.assertEquals(1000, ev.getTimestamp().getTime());
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testTimestampFromDate() throws ProcessorException {
        Event ev =  RunEtl("[@timestamp] = [b]", i -> i.put("b", new Date(1000)));
        Assert.assertEquals(1000, ev.getTimestamp().getTime());
        ev.remove("b");
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testMetaDirect() throws ProcessorException {
        Event ev =  RunEtl("[#a] = 1", i -> {});
        Assert.assertEquals(1, ev.getMeta("a"));
    }

    @Test
    public void testMetaToValueMove() throws ProcessorException {
        Event ev =  RunEtl("[a] < [#b]", i -> i.putMeta("b", 1));
        Assert.assertEquals(1, ev.get("a"));
        Assert.assertEquals(null, ev.getMeta("b"));
    }

    @Test
    public void testMetaToValueAssign() throws ProcessorException {
        Event ev =  RunEtl("[a] = [#b]", i -> i.putMeta("b", 1));
        Assert.assertEquals(1, ev.get("a"));
        Assert.assertEquals(1, ev.getMeta("b"));
    }

    @Test
    public void testValueToMeta() throws ProcessorException {
        Event ev =  RunEtl("[#a] < [b]", i -> i.put("b", 1));
        Assert.assertEquals(1, ev.getMeta("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testCastValue() throws ProcessorException {
        Event ev =  RunEtl("(java.lang.Integer) [a]", i -> i.put("a", "1"));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testCastMeta() throws ProcessorException {
        Event ev =  RunEtl("(java.lang.Integer) [#a]", i -> i.putMeta("a", "1"));
        Assert.assertEquals(1, ev.getMeta("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testFormatMeta() throws ProcessorException {
        Event ev =  RunEtl("[a]=\"${#1%s} ${#2%s}\"([#type], [type])", i -> {i.putMeta("type", 1);i.put("type", 2);} );
        Assert.assertEquals("1 2", ev.get("a"));
    }

    @Test
    public void testCastComplex() throws ProcessorException {
        Event ev =  RunEtl("[ #principal ] = ([ #principal ] =~ /([^@]+)(@.*)?/ )[1]", i -> i.putMeta("principal", "nobody"));
        Assert.assertEquals("nobody", ev.getMeta("principal"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testMetaChar() throws ProcessorException {
        Event ev =  RunEtl("[a] = \"\\\"'!\"", i -> {});
        Assert.assertEquals("\"'!", ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test(expected=RecognitionException.class)
    public void testContextReadOnly() throws ProcessorException {
        RunEtl("[@context principal] = 1", i -> {});
    }

}
