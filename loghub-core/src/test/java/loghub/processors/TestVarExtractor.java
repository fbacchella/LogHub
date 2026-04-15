package loghub.processors;

import java.beans.IntrospectionException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import loghub.BeanChecks;
import loghub.Expression;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestVarExtractor {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.VarExtractor");
    }

    @Test
    void test1() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setPath(VariablePath.parse("sub"));
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)[=:](?<value>[^;]+);?"));
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2;c");
        Assertions.assertTrue(e.process(t));
        @SuppressWarnings("unchecked")
        Map<String, Object> sub = (Map<String, Object>) e.get("sub");
        Assertions.assertEquals("1", sub.get("a"), "key a not found");
        Assertions.assertEquals("2", sub.get("b"), "key b not found");
        Assertions.assertEquals("c", e.get("message"), "key message not found");
    }

    @Test
    void test2() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)[=:](?<value>[^;]+);?"));
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2");
        e.process(t);
        Assertions.assertEquals("1", e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b found");
        Assertions.assertNull(e.get("message"), "key message found");
    }

    @Test
    void test3() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b:2;c");
        e.process(t);
        Assertions.assertEquals("1", e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b not found");
        Assertions.assertEquals("c", e.get("message"), "key message not found");
    }

    @Test
    void testMixed() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)=(?<value>[^;]+);?"));
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "noise a=1;b=2;error;c=3");
        e.process(t);
        Assertions.assertEquals("1", e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b not found");
        Assertions.assertEquals("3", e.get("c"), "key c not found");
        Assertions.assertEquals("noise error;", e.get("message"), "key message not found");
    }

    @Test
    void testCollisionList() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)=(?<value>[^;]+);?"));
        builder.setCollision(VarExtractor.Collision_handling.AS_LIST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assertions.assertEquals(List.of("1", "4"), e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b not found");
        Assertions.assertEquals("3", e.get("c"), "key c not found");
    }

    @Test
    void testCollisionListWrapped() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)=(?<value>[^;]+);?"));
        builder.setCollision(VarExtractor.Collision_handling.AS_LIST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        Event wrapped = e.wrap(VariablePath.parse("d1.d2"));
        wrapped.process(t);
        Assertions.assertEquals(List.of("1", "4"), e.getAtPath(VariablePath.of(List.of("d1", "d2", "a"))), "key a not found");
        Assertions.assertEquals("2", e.getAtPath(VariablePath.of(List.of("d1", "d2", "b"))), "key b not found");
        Assertions.assertEquals("3", e.getAtPath(VariablePath.of(List.of("d1", "d2", "c"))), "key c not found");
    }

    @Test
    void testCollisionFirst() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)=(?<value>[^;]+);?"));
        builder.setCollision(VarExtractor.Collision_handling.KEEP_FIRST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assertions.assertEquals("1", e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b not found");
        Assertions.assertEquals("3", e.get("c"), "key c not found");
    }

    @Test
    void testCollisionLast() throws ProcessorException {
        VarExtractor.Builder builder = VarExtractor.getBuilder();
        builder.setField(VariablePath.parse(".message"));
        builder.setParser(Pattern.compile("(?<name>[a-z]+)=(?<value>[^;]+);?"));
        builder.setCollision(VarExtractor.Collision_handling.KEEP_LAST);
        VarExtractor t = builder.build();

        Event e = factory.newEvent();
        e.put("message", "a=1;b=2;c=3;a=4");
        e.process(t);
        Assertions.assertEquals("4", e.get("a"), "key a not found");
        Assertions.assertEquals("2", e.get("b"), "key b not found");
        Assertions.assertEquals("3", e.get("c"), "key c not found");
    }

    @Test
    void test_loghub_processors_VarExtractor() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.VarExtractor"
                , BeanChecks.BeanInfo.build("parser", Pattern.class)
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

    @ParameterizedTest
    @MethodSource("providePatterns")
    void parsing(String patternPart) throws Throwable {
        String conf = """
            pipeline[main] {
                loghub.processors.VarExtractor {
                    parser: %s,
                    field: [message],
                }
            }
        """.formatted(patternPart);
        StringReader reader = new StringReader(conf);
        Properties p = Configuration.parse(reader);
        VarExtractor m = (VarExtractor) p.namedPipeLine.get("main").processors.stream().findFirst().get();

        Event event = factory.newEvent();
        event.put("message", "a=1");
        m.process(event);
        Assertions.assertEquals("1", event.get("a"));
    }

    static Stream<Arguments> providePatterns() {
        return Stream.of(
            Arguments.of("\"(?<name>[a-z]+)=(?<value>[^;]+)\""),
            Arguments.of("/(?<name>[a-z]+)=(?<value>[^;]+)/")
        );
    }

}
