package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Helpers;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestHierarchical {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    private Event process(Consumer<Hierarchical.Builder> configure, Function<Event, Event> fillEvent)
            throws ProcessorException {
        Hierarchical.Builder builder = Hierarchical.getBuilder();
        configure.accept(builder);
        Hierarchical hierarchical = builder.build();

        Properties props = new Properties(Collections.emptyMap());

        Assert.assertTrue(hierarchical.configure(props));

        Event e = factory.newEvent();
        e = fillEvent.apply(e);
        e.process(hierarchical);
        return e;
    }

    @Test
    public void testConfigurationParsing() throws IOException {
        String confile = "pipeline[hierarchy] {loghub.processors.Hierarchical {destination:[tmp], fields: [\"a.*\", \"b\"] }}";
        Properties props = Configuration.parse(new StringReader(confile));
        Hierarchical hierarchy = (Hierarchical) props.namedPipeLine.get("hierarchy").processors.get(0);
        Assert.assertEquals(VariablePath.of("tmp"), hierarchy.getDestination());
        Pattern[] patterns = hierarchy.getPatterns();
        Assert.assertEquals(2, patterns.length);
        Assert.assertEquals(Helpers.convertGlobToRegex("a.*").pattern(), patterns[0].pattern());
        Assert.assertEquals(Helpers.convertGlobToRegex("b").pattern(), patterns[1].pattern());
    }

    @Test
    public void testConfigurationParsingPath() throws IOException, InterruptedException, ProcessorException {
        String confile = "pipeline[hierarchy] {path[sub](loghub.processors.Hierarchical {destination: [.], fields: [\"a.*\", \"b\"] })}";
        Properties conf = Configuration.parse(new StringReader(confile));
        Event ev = factory.newEvent();
        Event sub = ev.wrap(VariablePath.of("sub"));
        sub.put("a.b", 1);
        sub.put("b", 2);
        sub.put("c", 3);
        Tools.runProcessing(ev, conf.namedPipeLine.get("hierarchy"), conf);
        conf.mainQueue.poll(1, TimeUnit.SECONDS);
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a.b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("b")));
        Assert.assertEquals(3, ev.getAtPath(VariablePath.of("sub.c")));
    }

    @Test
    public void TestHierarchy0() throws ProcessorException {
        Event ev = process(b -> {
                },
                e -> {
                    e.put("a.b", 1);
                    e.put("c.d", 2);
                    e.put("e", 3);
                    e.put("", 4);
                    return e;
                });
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a.b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("c.d")));
        Assert.assertEquals(3, ev.getAtPath(VariablePath.of("e")));
        Assert.assertEquals(4, ev.get(""));
    }

    @Test
    public void TestHierarchy1() throws ProcessorException {
        Event ev = process(b -> {
                    b.setDestination(VariablePath.of(new String[] {"tmp"}));
                    b.setFields(new String[]{"*.*"});
        },
                e -> {
                    e.put("a.b", 1);
                    e.put("c.d", 2);
                    e.put("e", 3);
                    e.put("", 4);
                    return e;
                });
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("tmp.a.b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("tmp.c.d")));
        Assert.assertEquals(3, ev.getAtPath(VariablePath.of("e")));
        Assert.assertEquals(4, ev.get(""));
    }

    @Test
    public void TestHierarchy2() throws ProcessorException {
        Event ev = process(b -> b.setFields(new String[]{"*"}),
                e -> {
                    e.put("a.b", 1);
                    e.put("c.d", 2);
                    e.put("e.", 3);
                    e.put("f", 4);
                    e.put("", 5);
                    return e;
                });
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a.b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("c.d")));
        Assert.assertEquals(3, ev.get("e"));
        Assert.assertEquals(4, ev.getAtPath(VariablePath.of("f")));
        Assert.assertEquals(5, ev.get(""));
    }

    @Test
    public void TestHierarchy3() throws ProcessorException {
        Event ev = process(b -> {
                    b.setDestination(VariablePath.of(new String[] {"tmp"}));
                    b.setFields(new String[]{"a.*"});
                },
                e -> {
                    e.put("a.b", 1);
                    e.put("c.d", 2);
                    e.put("e", 3);
                    e.put("", 4);
                    return e;
                });
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("tmp.a.b")));
        Assert.assertEquals(2, ev.get("c.d"));
        Assert.assertEquals(3, ev.getAtPath(VariablePath.of("e")));
        Assert.assertEquals(4, ev.get(""));
    }

    @Test
    public void TestHierarchy4() throws ProcessorException {
        Event ev = process(b -> {
            b.setFields(new String[] {"*"});
            b.setDestination(VariablePath.of("."));
        },
                e -> {
                    e.putAtPath(VariablePath.of(new String[]{"s", "a.b"}), 1);
                    e.putAtPath(VariablePath.of(new String[]{"s", "c.d"}), 2);
                    e.putAtPath(VariablePath.of(new String[]{"e"}), 3);
                    return e.wrap(VariablePath.of("s"));
                });
        ev = ev.unwrap();
        Assert.assertEquals(1, ev.getAtPath(VariablePath.of("a.b")));
        Assert.assertEquals(2, ev.getAtPath(VariablePath.of("c.d")));
        Assert.assertEquals(3, ev.get("e"));
        Assert.assertTrue(((Map<?, ?>)ev.getAtPath(VariablePath.of("s"))).isEmpty());
    }

    @Test
    public void test_loghub_processors_Hierarchical() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Hierarchical"
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
        );
    }

}
