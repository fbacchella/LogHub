package loghub.processors;

import java.io.StringReader;
import java.util.Collections;
import java.util.List;
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
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestSplit {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @ParameterizedTest
    @MethodSource("provideSplittingCases")
    void testSplitting(String pattern, boolean keepempty, String message, List<String> expected) throws ProcessorException {
        List<String> result = test(pattern, keepempty, message);
        Assertions.assertEquals(expected, result);
    }

    static Stream<Arguments> provideSplittingCases() {
        return Stream.of(
            Arguments.of(",", true, "a,b,c", List.of("a", "b", "c")),
            Arguments.of(",", true, ",a,b,c,", List.of("", "a", "b", "c", "")),
            Arguments.of(",", false, ",a,b,c,", List.of("a", "b", "c")),
            Arguments.of("#", false, "#a#b#c#", List.of("a", "b", "c")),
            Arguments.of(",", false, "a", List.of("a"))
        );
    }

    @SuppressWarnings("unchecked")
    private List<String> test(String pattern, boolean keepempty, String message) throws ProcessorException {
        Split.Builder builder = Split.getBuilder();
        builder.setPattern(Pattern.compile(pattern));
        builder.setKeepempty(keepempty);
        Split parse = builder.build();
        parse.setField(VariablePath.parse("field"));
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));
        Event event = factory.newEvent();
        event.put("field", message);
        parse.process(event);
        return (List<String>) event.get("field");
    }

    @Test
    void testBeans() throws ReflectiveOperationException, java.beans.IntrospectionException {
        BeanChecks.beansCheck(logger, "loghub.processors.Split"
                , BeanChecks.BeanInfo.build("pattern", Pattern.class)
                , BeanChecks.BeanInfo.build("destination", VariablePath.class)
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
                loghub.processors.Split {
                    pattern: %s,
                    field: [message],
                }
            }
        """.formatted(patternPart);
        StringReader reader = new StringReader(conf);
        Properties p = Configuration.parse(reader);
        Split m = (Split) p.namedPipeLine.get("main").processors.stream().findFirst().get();

        Event event = factory.newEvent();
        event.put("message", "a,b,c");
        m.process(event);
        @SuppressWarnings("unchecked")
        List<String> elements = (List<String>) event.get("message");
        Assertions.assertEquals(List.of("a", "b", "c"), elements);
    }

    static Stream<Arguments> providePatterns() {
        return Stream.of(
            Arguments.of("\",\""),
            Arguments.of("/,/")
        );
    }

}
