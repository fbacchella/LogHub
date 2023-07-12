package loghub.processors;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.EventsProcessor;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.RouteParser;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigurationTools;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestEtl {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Etl", "loghub.EventsProcessor", "loghub.Expression");
    }

    private Event RunEtl(String exp, Consumer<Event> filer) throws ProcessorException {
        return RunEtl(exp, filer, true);
    }

    private Event RunEtl(String exp, Consumer<Event> filer, boolean status) throws ProcessorException {
        return RunEtl(exp, filer, status, null);
    }

    private Event RunEtl(String exp, Consumer<Event> filer, boolean status, CompletableFuture<Event> holder) throws ProcessorException {
        Map<String, VarFormatter> formatters = new HashMap<>();
        Etl e =  ConfigurationTools.buildFromFragment(exp, RouteParser::etl, formatters);
        Map<String, Object> settings = new HashMap<>(1);
        settings.put("__FORMATTERS", formatters);
        e.configure(new Properties(settings));
        Event ev = factory.newEvent();
        if (holder != null) {
            holder.complete(ev);
        }
        filer.accept(ev);
        Assert.assertEquals(status, e.process(ev));
        return ev;
    }

    private Etl parseEtl(String exp) {
        Etl e =  ConfigurationTools.buildFromFragment(exp, RouteParser::etl);
        e.configure(new Properties(Collections.emptyMap()));
        return e;
    }

    @Test
    public void test1() throws ProcessorException, Expression.ExpressionException {
        Properties props = new Properties(Collections.emptyMap());
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue(VariablePath.of("a", "b"));
        etl.setExpression(new Expression("event.c + 1", props.groovyClassLoader, props.formatters));
        boolean done = etl.configure(props);
        Assert.assertTrue("configuration failed", done);
        Event event = factory.newEvent();
        event.put("c", 0);
        event.process(etl);
        Assert.assertEquals("evaluation failed", 1, event.getAtPath(VariablePath.of("a", "b")));
    }

    @Test
    public void test2() throws ProcessorException {
        Etl etl = new Etl.Remove();
        etl.setLvalue(VariablePath.of("a"));
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = factory.newEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", NullOrMissingValue.MISSING, event.getAtPath(VariablePath.of("a")));
    }

    @Test
    public void test3() throws ProcessorException {
        Etl.Rename etl = new Etl.Rename();
        etl.setLvalue(VariablePath.of("b"));
        etl.setSource(VariablePath.of("a"));
        boolean done = etl.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue("configuration failed", done);
        Event event = factory.newEvent();
        event.put("a", 0);
        etl.process(event);
        Assert.assertEquals("evaluation failed", 0, event.getAtPath(VariablePath.of("b")));
    }

    @Test
    public void test4() throws ProcessorException, Expression.ExpressionException {
        Map<String, VarFormatter> formats = Collections.singletonMap("a", new VarFormatter("${%t<GMT>H}"));
        Map<String, Object> properties = new HashMap<>();
        properties.put("__FORMATTERS", formats);
        Properties props = new Properties(properties);
        Etl.Assign etl = new Etl.Assign();
        etl.setLvalue(VariablePath.of("a"));
        etl.setExpression(new Expression("formatters.a.format(event.getTimestamp())", props.groovyClassLoader, props.formatters));
        boolean done = etl.configure(props);
        Assert.assertTrue("configuration failed", done);
        Event event = factory.newEvent();
        event.setTimestamp(new Date(3600 * 1000));
        event.process(etl);
        Assert.assertEquals("evaluation failed", "01", event.get("a"));
    }

    @Test
    public void test5() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.put("a", "a");

        Tools.runProcessing(sent, conf.namedPipeLine.get("main"), conf);

        Assert.assertEquals("conversion not expected", "a", sent.get("a"));
    }

    @Test
    public void test7() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        Map<String, Object> b = new HashMap<>(1);
        b.put("c", 1);
        sent.put("b", b);

        Tools.runProcessing(sent, conf.namedPipeLine.get("third"), conf);
        Assert.assertEquals("conversion not expected", 1, sent.get("a"));
    }

    @Test
    public void test8() throws ProcessorException, ConfigException, IOException {
        Properties conf = Tools.loadConf("etl.conf");
        Helpers.parallelStartProcessor(conf);
        Event sent = factory.newEvent();
        sent.setTimestamp(new Date(1));
        Tools.runProcessing(sent, conf.namedPipeLine.get("timestamp"), conf);
        Assert.assertEquals(new Date(0), sent.getTimestamp());
        Assert.assertEquals(new Date(1), sent.get("reception_time"));
    }

    @Test
    public void testAssign() throws ProcessorException {
        Etl e = parseEtl("[a] = 1");
        Event ev = factory.newEvent();
        Assert.assertTrue(e.process(ev));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testElementMissing() throws InterruptedException, ExecutionException {
        CompletableFuture<Event> holder = new CompletableFuture<>();
        Assert.assertThrows(IgnoredEventException.class, () -> RunEtl("[b] = [a]", i -> {}, true, holder));
        Assert.assertTrue(holder.get().isEmpty());
    }

    @Test
    public void testPathMissing() throws InterruptedException, ExecutionException {
        CompletableFuture<Event> holder = new CompletableFuture<>();
        Assert.assertThrows(IgnoredEventException.class, () -> RunEtl("[c] = [a b]", i -> {}, true, holder));
        Assert.assertTrue(holder.get().isEmpty());
    }

    @Test
    public void testElementNull() throws ProcessorException {
        Event ev =  RunEtl("[b] = [a]", i -> i.put("a", NullOrMissingValue.NULL));
        Assert.assertTrue(ev.containsKey("b"));
        Assert.assertNull(ev.get("b"));
        // Ensure that JUnis is not doing any magic tricks when checking NotNull
        Assert.assertNotNull(NullOrMissingValue.NULL);

    }

    @Test
    public void testAssignIndirect() throws ProcessorException {
        Etl e = parseEtl("[<- a] = 1");
        Event ev = factory.newEvent();
        ev.put("a", "b");
        Assert.assertTrue(e.process(ev));
        Assert.assertEquals(1, ev.remove("b"));
        Assert.assertEquals("b", ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testAssignIndirectValue() throws ProcessorException {
        Etl e = parseEtl("[a] = [<- b]");
        Event ev = factory.newEvent();
        ev.put("b", "c");
        ev.put("c", 1);
        Assert.assertTrue(e.process(ev));
        Assert.assertEquals("c", ev.remove("b"));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertEquals(1, ev.remove("c"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testRenameIndirectValue() throws ProcessorException {
        Etl e = parseEtl("[a] < [<- b]");
        Event ev = factory.newEvent();
        ev.put("b", "c");
        ev.put("c", 1);
        Assert.assertTrue(e.process(ev));
        Assert.assertEquals("c", ev.remove("b"));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testAssignIndirectMissing() throws ProcessorException {
        Etl e = parseEtl("[<- a] = 1");
        Event ev = factory.newEvent();
        Assert.assertTrue(e.process(ev));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testRename() throws ProcessorException {
        Event ev =  RunEtl("[a] < [b]", i -> i.put("b", 1));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testRenameIndirecDeep() throws ProcessorException {
        Map<String, Object> amap = Collections.singletonMap("b", "c");
        Event ev =  RunEtl("[<- a b] < [d]", i -> {
            i.put("a", amap);
            i.put("d", 1);
        });
        Assert.assertEquals(1, ev.remove("c"));
        Assert.assertNull(ev.remove("d"));
        Assert.assertSame(amap, ev.remove("a"));
        Assert.assertNull(ev.remove("a"));
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
    public void testTimestampFromInstant() throws ProcessorException {
        Event ev =  RunEtl("[@timestamp] = [b]", i -> i.put("b", Instant.ofEpochMilli(1000)));
        Assert.assertEquals(1000, ev.getTimestamp().getTime());
        ev.remove("b");
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testTimestampFromNumber() throws ProcessorException {
        Event ev =  RunEtl("[@timestamp] = [b]", i -> i.put("b", 1000));
        Assert.assertEquals(1000, ev.getTimestamp().getTime());
        ev.remove("b");
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
        Assert.assertEquals(NullOrMissingValue.MISSING, ev.getMeta("b"));
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
    public void testConvert() throws ProcessorException {
        Event ev =  RunEtl("(java.lang.Integer) [a]", i -> i.put("a", "1"));
        Assert.assertEquals(1, ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test
    public void testConvertNull() {
        Assert.assertThrows(loghub.IgnoredEventException.class, () -> RunEtl("(java.lang.Integer) [a]", i -> {}, false));
    }

    @Test
    public void testConvertNullPath() {
        Assert.assertThrows(loghub.IgnoredEventException.class, () -> RunEtl("(java.lang.Integer) [a b]", i -> {}, false));
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
        // The expected string is "'!
        Event ev =  RunEtl("[a] = \"\\\"'!\"", i -> {});
        Assert.assertEquals("\"'!", ev.remove("a"));
        Assert.assertTrue(ev.isEmpty());
    }

    @Test(expected=RecognitionException.class)
    public void testContextReadOnly() throws ProcessorException {
        RunEtl("[@context principal] = 1", i -> {});
    }

    @Test
    public void testMappingNull() throws InterruptedException, ExecutionException {
        CompletableFuture<Event> holder = new CompletableFuture<>();
        Assert.assertThrows(IgnoredEventException.class, () -> RunEtl("[ a b ] @ [ a b ] {0: 1} ", i -> {}, true, holder));
        Assert.assertTrue(holder.get().isEmpty());
    }

    @Test
    public void testAppend() throws ProcessorException {
        // Comprehensive type testing is done in loghub.TestEvent#testAppend()
        Event ev1 =  RunEtl("[a] =+ 1", i -> i.put("a", new int[]{0}));
        int[] v1 = (int[]) ev1.get("a");
        Assert.assertArrayEquals(new int[]{0, 1}, v1);

        Event ev2 =  RunEtl("[a] =+ 1", i -> i.put("a", new Number[]{0L}));
        Number[] v2 = (Number[]) ev2.get("a");
        Assert.assertArrayEquals(new Number[]{0L, 1}, v2);

        Event ev3 =  RunEtl("[a] =+ 1", i -> i.put("a", new ArrayList<>(List.of("0"))));
        List<Object> v3 = (List<Object>) ev3.get("a");
        Assert.assertEquals(List.of("0", 1), v3);

        Event ev4 =  RunEtl("[a] =+ 1", i -> {});
        List<Object> v4 = (List<Object>) ev4.get("a");
        Assert.assertEquals(List.of(1), v4);

        Assert.assertThrows(IgnoredEventException.class,
                            () -> RunEtl("[a] =+ [b]", i -> i.put("a", new ArrayList<>(List.of("0")))));
    }

}
