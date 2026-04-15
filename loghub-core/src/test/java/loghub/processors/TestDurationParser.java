package loghub.processors;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import loghub.ProcessorException;
import loghub.Tools;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

class TestDurationParser {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
    }

    @Test
    void testFull() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<days>\\d+)d(?<hours>\\d+)h(?<minutes>\\d+)m(?<seconds>\\d+)s");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1d2h3m4s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), duration);
    }

    @Test
    void testPartial() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // Only minute and second
        builder.setPattern("(?<minutes>\\d+)m(?<seconds>\\d+)s");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "3m4s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofMinutes(3).plusSeconds(4), duration);
    }

    @Test
    void testOptionalGroups() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // Groups are present in regex but maybe not in input
        builder.setPattern("((?<days>\\d+)d)?((?<hours>\\d+)h)?((?<minutes>\\d+)m)?((?<seconds>\\d+)s)?");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "2h4s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofHours(2).plusSeconds(4), duration);
    }

    @Test
    void testFloatSeconds() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<seconds>\\d+\\.\\d+)s");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1.5s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofSeconds(1, 500_000_000), duration);
    }

    @Test
    void testHighPrecisionFloat() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<seconds>\\d+\\.\\d+)s");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        // 1.23456789123 seconds -> rounded to nanos
        event.put("field", "1.123456789123s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofSeconds(1, 123_456_789), duration);
    }

    @Test
    void testSecondKeyInPatternButNotInput() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // Pattern defines "seconds" but it's optional and not in input
        builder.setPattern("((?<minutes>\\d+)m)?((?<seconds>\\d+)s)?");
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
        builder.setPattern("(?<minutes>\\d+)m");
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
    void testMilliseconds() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<seconds>\\d+)s(?<milliseconds>\\d+)ms");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1s500ms");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofSeconds(1, 500_000_000), duration);
    }

    @Test
    void testMillisecondsOnly() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<milliseconds>\\d+)ms");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1234ms");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        Assertions.assertEquals(Duration.ofMillis(1234), duration);
    }

    @Test
    void testFloatAndMilliseconds() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        // C'est un cas un peu étrange mais supporté par le code : les deux s'additionnent
        builder.setPattern("(?<seconds>\\d+\\.\\d+)s(?<milliseconds>\\d+)ms");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "1.5s500ms");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        // 1.5s = 1s 500ms, + 500ms = 2s
        Assertions.assertEquals(Duration.ofSeconds(2), duration);
    }

    @Test
    void testNoMatch() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<days>\\d+)d");
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
        builder.setPattern("(?<days>\\d+)d");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", null);
        parse.process(event);
        Assertions.assertNull(event.get("field"));
    }

    @Test
    void testNegativeSeconds() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<seconds>-?\\d+\\.\\d+)s");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "-1.5s");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        // -1.5s = -1s -500ms = -2s + 500ms (en termes de seconds/nanos dans java.time.Duration)
        // Duration.ofSeconds(-1, -500_000_000) ou Duration.ofSeconds(-2, 500_000_000) sont équivalents
        Assertions.assertEquals(Duration.ofMillis(-1500), duration);
    }

    @Test
    void testNegativeAndPositiveGroups() throws ProcessorException {
        DurationParser.Builder builder = DurationParser.getBuilder();
        builder.setPattern("(?<hours>-?\\d+)h(?<minutes>-?\\d+)m");
        builder.setField(VariablePath.parse("field"));
        DurationParser parse = builder.build();
        Assertions.assertTrue(parse.configure(new Properties(Collections.emptyMap())));

        Event event = factory.newEvent();
        event.put("field", "-1h30m");
        parse.process(event);
        Duration duration = (Duration) event.get("field");
        // -1h + 30m = -30m
        Assertions.assertEquals(Duration.ofMinutes(-30), duration);
    }

}
