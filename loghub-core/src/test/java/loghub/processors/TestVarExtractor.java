package loghub.processors;

import java.beans.IntrospectionException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestVarExtractor {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.VarExtractor");
    }

    @Test
    public void test1() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setPath(VariablePath.parse("sub"));
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2;c");
        Assert.assertTrue(e.process(t));
        @SuppressWarnings("unchecked")
        Map<String, Object> sub = (Map<String, Object>) e.get("sub");
        Assert.assertEquals("key a not found", "1", sub.get("a"));
        Assert.assertEquals("key b not found", "2", sub.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));
    }

    @Test
    public void test2() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b found", "2", e.get("b"));
        Assert.assertNull("key message found", e.get("message"));
    }

    @Test
    public void test3() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2;c");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));
    }

    @Test
    public void testMixed() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)=(?<value>[^;]+);?");
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "noise a=1;b=2;error;c=3");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key c not found", "3", e.get("c"));
        Assert.assertEquals("key message not found", "noise error;", e.get("message"));
    }

    @Test
    public void testCollisionList() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)=(?<value>[^;]+);?");
        builder.setCollision(VarExtractor.Collision_handling.AS_LIST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assert.assertEquals("key a not found", List.of("1", "4"), e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key c not found", "3", e.get("c"));
    }

    @Test
    // Needs an explicit test because AS_LIST uses merge, that needs to be overridden in EventWrapper
    public void testCollisionListWrapped() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)=(?<value>[^;]+);?");
        builder.setCollision(VarExtractor.Collision_handling.AS_LIST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        Event wrapped = e.wrap(VariablePath.parse("d1.d2"));
        wrapped.process(t);
        Assert.assertEquals("key a not found", List.of("1", "4"), e.getAtPath(VariablePath.of(List.of("d1", "d2", "a"))));
        Assert.assertEquals("key b not found", "2", e.getAtPath(VariablePath.of(List.of("d1", "d2", "b"))));
        Assert.assertEquals("key c not found", "3", e.getAtPath(VariablePath.of(List.of("d1", "d2", "c"))));
    }

    @Test
    public void testCollisionFirst() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)=(?<value>[^;]+);?");
        builder.setCollision(VarExtractor.Collision_handling.KEEP_FIRST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key c not found", "3", e.get("c"));
    }

    @Test
    public void testCollisionLast() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser("(?<name>[a-z]+)=(?<value>[^;]+);?");
        builder.setCollision(VarExtractor.Collision_handling.KEEP_LAST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assert.assertEquals("key a not found", "4", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key c not found", "3", e.get("c"));
    }

    @Test
    public void test_loghub_processors_VarExtractor() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.VarExtractor"
                , BeanChecks.BeanInfo.build("parser", String.class)
                , BeanChecks.BeanInfo.build("collision", VarExtractor.Collision_handling.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
                , BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class)
                , BeanChecks.BeanInfo.build("field", VariablePath.class)
                , BeanChecks.BeanInfo.build("fields", String[].class)
                , BeanChecks.BeanInfo.build("path", VariablePath.class)
                , BeanChecks.BeanInfo.build("if", Expression.class)
                , BeanChecks.BeanInfo.build("success", Processor.class)
                , BeanChecks.BeanInfo.build("failure", Processor.class)
                , BeanChecks.BeanInfo.build("exception", Processor.class)
        );
    }

}
