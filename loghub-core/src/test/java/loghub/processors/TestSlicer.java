package loghub.processors;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestSlicer {

    private static final Logger logger = LogManager.getLogger();
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.processors.Slicer", "loghub.EventsProcessor");
    }

    @Test
    public void testDoNothing() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(
                VariablePath.of("a"), List.of(java.util.Map.of("b", "1"), Map.of("b", "2"))
        );
        PriorityBlockingQueue main = run(new StringReader("pipeline[main]{ loghub.processors.Slicer{ toSlice: [b] } }"), populate);
        Assert.assertTrue(main.isEmpty());
    }

    @Test
    public void testSingle() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(
                VariablePath.of("a"), List.of(java.util.Map.of("b", "1"), Map.of("b", "2"))
        );
        PriorityBlockingQueue main = run(new StringReader("pipeline[main]{ loghub.processors.Slicer{ toSlice: [a], bucket: now, flatten: true } }"), populate);
        Event ev1 = main.poll();
        Assert.assertEquals("1", ev1.getAtPath(VariablePath.of("a", "b")));
        Event ev2 = main.poll();
        Assert.assertEquals("2", ev2.getAtPath(VariablePath.of("a", "b")));
    }

    @Test
    public void testSingleDefault() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(
                VariablePath.of("a"), List.of(java.util.Map.of("b", "1"), Map.of("b", "2"))
        );
        PriorityBlockingQueue main = run(new StringReader("pipeline[main]{ loghub.processors.Slicer{ toSlice: [a] } }"), populate);
        Event ev1 = main.poll();
        Assert.assertEquals(List.of(Map.of("b", "1")), ev1.getAtPath(VariablePath.of("a")));
        Event ev2 = main.poll();
        Assert.assertEquals(List.of(Map.of("b", "2")), ev2.getAtPath(VariablePath.of("a")));
    }

    @Test
    public void testMany() throws IOException, InterruptedException {
        Consumer<Event> populate = ev -> ev.putAtPath(
                VariablePath.of("a"), List.of(java.util.Map.of("b", 1), Map.of("b", 2), Map.of("b", 3), Map.of("b", 4))
        );
        PriorityBlockingQueue main = run(new StringReader("pipeline[main]{ loghub.processors.Slicer{ toSlice: [a], bucket: [b] % 2 } }"), populate);
        Event ev1 = main.poll();
        Assert.assertEquals(List.of(Map.of("b", 1), Map.of("b", 3)), ev1.getAtPath(VariablePath.of("a")));
        Event ev2 = main.poll();
        Assert.assertEquals(List.of(Map.of("b", 2), Map.of("b", 4)), ev2.getAtPath(VariablePath.of("a")));
    }

    public PriorityBlockingQueue run(Reader r, Consumer<Event> populate)
            throws IOException, InterruptedException {
        Properties props = Tools.loadConf(r);
        Event ev = factory.newEvent();
        populate.accept(ev);
        Tools.runProcessing(ev, props.namedPipeLine.get("main"), props);
        Assert.assertEquals(ev, props.mainQueue.poll(5, TimeUnit.SECONDS));
        return props.mainQueue;
    }

    @Test
    public void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Slicer"
                , BeanChecks.BeanInfo.build("bucket", Expression.class)
                , BeanChecks.BeanInfo.build("toSlice", VariablePath.class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
