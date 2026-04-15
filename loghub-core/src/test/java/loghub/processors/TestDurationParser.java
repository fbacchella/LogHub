package loghub.processors;

import java.io.StringReader;
import java.time.Duration;
import java.util.Collections;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestDurationParser {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
    }

    @ParameterizedTest
    @MethodSource("provideSimpleDurations")
    void testDurations(String regex, String input, Duration expected) throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern(Pattern.compile(regex));
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", input);
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(expected, duration);
    }

    static Stream<Arguments> provideSimpleDurations() {
        return Stream.of(
            // Full
            Arguments.of("(?<days>\\d+)d(?<hours>\\d+)h(?<minutes>\\d+)m(?<seconds>\\d+)s", "1d2h3m4s", Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4)),
            // Partial
            Arguments.of("(?<minutes>\\d+)m(?<seconds>\\d+)s", "3m4s", Duration.ofMinutes(3).plusSeconds(4)),
            // Optional groups
            Arguments.of("((?<days>\\d+)d)?((?<hours>\\d+)h)?((?<minutes>\\d+)m)?((?<seconds>\\d+)s)?", "2h4s", Duration.ofHours(2).plusSeconds(4)),
            // Float seconds
            Arguments.of("(?<seconds>\\d+\\.\\d+)s", "1.5s", Duration.ofSeconds(1, 500_000_000)),
            // High precision
            Arguments.of("(?<seconds>\\d+\\.\\d+)s", "1.123456789123s", Duration.ofSeconds(1, 123_456_789)),
            // Milliseconds
            Arguments.of("(?<seconds>\\d+)s(?<milliseconds>\\d+)ms", "1s500ms", Duration.ofSeconds(1, 500_000_000)),
            // Milliseconds only
            Arguments.of("(?<milliseconds>\\d+)ms", "1234ms", Duration.ofMillis(1234)),
            // Float and milliseconds
            Arguments.of("(?<seconds>\\d+\\.\\d+)s(?<milliseconds>\\d+)ms", "1.5s500ms", Duration.ofSeconds(2)),
            // Negative seconds
            Arguments.of("(?<seconds>-?\\d+\\.\\d+)s", "-1.5s", Duration.ofMillis(-1500)),
            // Negative and positive groups
            Arguments.of("(?<hours>-?\\d+)h(?<minutes>-?\\d+)m", "-1h30m", Duration.ofMinutes(-30))
        );
    }

    @Test
    void testSecondKeyInPatternButNotInput() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // Pattern defines "seconds" but it's optional and not in input
        builder.setPattern(Pattern.compile("((?<minutes>\\d+)m)?((?<seconds>\\d+)s)?"));
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "5m");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofMinutes(5), duration);
    }

    @Test
    void testSecondKeyNotInPattern() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // Pattern does NOT define "seconds"
        builder.setPattern(Pattern.compile("(?<minutes>\\d+)m"));
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "5m");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofMinutes(5), duration);
    }

    @Test
    void testNoMatch() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern(Pattern.compile("(?<days>\\d+)d"));
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "no match");
        boolean result = parse.process(event);
        Assertions.assertFalse(result);
        Assertions.assertEquals("no match", event.get("field"));
    }

    @Test
    void testNullInput() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern(Pattern.compile("(?<days>\\d+)d"));
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", null);
        parse.process(event);
        Assertions.assertNull(event.get("field"));
    }

    @ParameterizedTest
    @MethodSource("providePatterns")
    void parsing(String patternPart) throws Throwable {
        String conf = """
            pipeline[main] {
                loghub.processors.DurationParser {
                    pattern: %s,
                    field: [message],
                }
            }
        """.formatted(patternPart);
        StringReader reader = new StringReader(conf);
        Properties p = Configuration.parse(reader);
        DurationParser m = (DurationParser) p.namedPipeLine.get("main").processors.stream().findFirst().get();

        Event event = factory.newEvent();
        event.put("message", "1+2:3:4");
        m.process(event);
        Duration duration = (Duration) event.get("message");
        Assertions.assertEquals(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), duration);
    }

    static Stream<Arguments> providePatterns() {
        return Stream.of(
            Arguments.of("\"(?<days>\\\\d+)\\\\+(?<hours>\\\\d+):(?<minutes>\\\\d+):(?<seconds>\\\\d+)\""),
            Arguments.of("/((?<days>\\d+)\\+)(?<hours>\\d+):(?<minutes>\\d+):(?<seconds>\\d+)/")
        );
    }

}
