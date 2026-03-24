package loghub.processors;

import java.beans.IntrospectionException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

class TestDissect {

    private static Logger logger;
    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    static void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors");
    }

    @Test
    void test() throws ProcessorException {
        dobuild("%{a} %{b} %{c}", "1 2 3",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));

        // Right padding modifier ->
        dobuild("%{a->} %{b} %{c}", "1   2 3",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{a->},%{b},%{c}", "1,,,,2,3",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{a->},:%{b},%{c}", "1,:,:,:,:2,3",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{->},%{b},%{c}", "1,,,,2,3", Map.ofEntries(Map.entry("b", "2"), Map.entry("c", "3")));

        // Append modifier +
        dobuild("%{a} %{+a} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "2", "3" })));
        dobuild("%{a} %{+a} %{+a}", b -> b.setAppendSeparator(", "), "1 2 3", Map.ofEntries(Map.entry("a", "1, 2, 3")));

        // Append modifier with order + with /n
        dobuild("%{a} %{+a/2} %{+a/1}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "3", "2" })));

        // Mixed append: add(foundValue) and set(key.appendModifier, foundValue)
        // first +a adds to index 0, then +a/1 sets index 1, then +a adds to index 1 (overwriting!)
        // WAIT: let's see what happens.
        // %{+a} -> add("1"), last=1, data=[1, null, null]
        // %{+a/1} -> set(1, "2"), data=[1, 2, null]
        // %{+a} -> add("3"), last=1, data=[1, 3, null]
        dobuild("%{+a} %{+a/1} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "3", null })));
        // Another mixed case: +a/2 sets index 2, +a adds to index 0, +a adds to index 1
        // %{+a/2} -> set(2, "1"), data=[null, null, 1]
        // %{+a} -> add("2"), last=0, data=[2, null, 1], last=1
        // %{+a} -> add("3"), last=1, data=[2, 3, 1], last=2
        dobuild("%{+a/2} %{+a} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "2", "3", "1" })));

        // Named skip key ?
        dobuild("%{a} %{?skipme} %{c}", "1 2 3", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "3")));
        dobuild("%{a} %{?skipme} %{?skipme} %{c}", "1 2 3 4", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "4")));

        // Reference keys * and &
        dobuild("%{*a} %{b} %{&a}", "c 1 2", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2")));
        dobuild("%{&a} %{b} %{*a}", "2 1 c", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2")));

        // Remaining match
        dobuild("%{a} %{b},%{c}", "1 2,3  4",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3  4")));

        // Consecutive repeating delimiters
        dobuild("%{a},%{b},%{c},%{d}", "1,,,4",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", ""), Map.entry("d", "4")));
        dobuild("%{a},%{b},%{c},%{d},%{e},%{f},%{g}", "1,,2,,,,3",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", "2"), Map.entry("d", ""),
                        Map.entry("e", ""), Map.entry("f", ""), Map.entry("g", "3")));

        // Postfix pattern
        dobuild("%{timestamp} %{+timestamp} %{+timestamp} %{logsource} %{program}[%{pid}]: %{message}",
                b -> b.setAppendSeparator(" "),
                "Mar 16 00:01:25 example postfix/smtpd[1713]: connect from example.com[192.100.1.3]",
                Map.ofEntries(Map.entry("timestamp", "Mar 16 00:01:25"), Map.entry("pid", "1713"),
                        Map.entry("program", "postfix/smtpd"), Map.entry("logsource", "example"),
                        Map.entry("message", "connect from example.com[192.100.1.3]")));

        // More tests
        dobuild("0%{a} %{b} %{c}4", "01 2 34",
                Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3")));
        dobuild("%{+a/2} %{+a/1} %{+a/0}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "3", "2", "1" })));

        // Pattern starting/ending with fixed string
        dobuild("start %{a} end", "start 1 end", Map.ofEntries(Map.entry("a", "1")));

        // Complex padding case: padding at the end of the chain
        dobuild("%{a->} ", "1   ", Map.ofEntries(Map.entry("a", "1")));

        // Field names with dots (already tested in inPlaceWithPath, but let's do it in dobuild)
        dobuild("%{a.b} %{c.d}", "1 2", Map.ofEntries(Map.entry("a.b", "1"), Map.entry("c.d", "2")));
    }

    @Test
    void fails() throws ProcessorException {
        checkException(IllegalArgumentException.class, "%{/2}", "Append order defined, without append modifier");
        checkException(IllegalArgumentException.class, "%{a:nothing}", "Unknown conversion type: nothing");
        checkException(ProcessorException.class, "%{a:int}", "Can not convert \"a\" to int");
        checkException(IllegalArgumentException.class, "%{a} %{+a/2}", "Appender modifier out of range for key \"a\"");
        checkException(IllegalArgumentException.class, "%{a} %{*a}", "Reference key \"a\" must be specified as pair");
        checkException(IllegalArgumentException.class, "%{*a} %{*a}",
                "Needs a reference name and a reference value for key \"a\"");
        checkException(IllegalArgumentException.class, "%{&a} %{&a}",
                "Needs a reference name and a reference value for key \"a\"");
        checkException(IllegalArgumentException.class, "%{*a:int} %{&a}",
                "A type conversion can't be applied to a reference name");

        dobuild("prefix%{a} %{b}", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
        dobuild("%{a} %{b}suffix", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
        dobuild("%{a} %{b} %{c}", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
        // Missing separator at the beginning
        dobuild("prefix%{a}", "1", FieldsProcessor.RUNSTATUS.FAILED);
        // Missing intermediate separator
        dobuild("%{a},%{b}", "1 2", FieldsProcessor.RUNSTATUS.FAILED);
    }

    private <E extends Exception> void checkException(Class<E> eClass, String pattern, String expectedMessage) {
        checkException(eClass, pattern, "", expectedMessage);
    }

    private <E extends Exception> void checkException(Class<E> eClass, String pattern, String message,
            String expectedMessage) {
        E ex = Assertions.assertThrows(eClass, () -> dobuild(pattern, message, Map.ofEntries()));
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    void convert() throws ProcessorException, UnknownHostException {
        dobuild("%{a:int} %{b:ip} %{c:boolean}", "1 8.8.8.8 true",
                Map.ofEntries(Map.entry("a", 1), Map.entry("b", InetAddress.getByName("8.8.8.8")),
                        Map.entry("c", true)));
    }

    @Test
    void inPlaceWithPath() throws ProcessorException {
        Dissect.Builder builder = Dissect.getBuilder();
        builder.setPattern("%{a.b} %{c.d} %{#meta}");
        builder.setInPlace(true);
        builder.setField(VariablePath.of("message"));
        Dissect dissector = builder.build();
        Event ev = factory.newEvent();
        ev.put("message", "1 2 3");
        ev.process(dissector);
        Assertions.assertEquals("1", ev.getAtPath(VariablePath.parse("a.b")));
        Assertions.assertEquals("2", ev.getAtPath(VariablePath.parse("c.d")));
        Assertions.assertEquals("3", ev.getAtPath(VariablePath.parse("#meta")));
    }

    private void dobuild(String pattern, Consumer<Dissect.Builder> configure, String value, Object expected,
            Consumer<Object> verification) throws ProcessorException {
        Dissect.Builder builder = Dissect.getBuilder();
        builder.setPattern(pattern);
        builder.setInPlace(false);
        configure.accept(builder);
        Dissect dissector = builder.build();
        Event ev = factory.newEvent();
        Object values = dissector.fieldFunction(ev, value);
        if (values instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> mapValues = (Map<String, Object>) values;
            values = mapValues.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                    e -> e.getValue() instanceof Object[] ? Arrays.asList((Object[]) e.getValue()) : e.getValue()));
        }
        if (expected instanceof Map) {
            @SuppressWarnings("unchecked") Map<String, Object> mapExpected = (Map<String, Object>) expected;
            expected = mapExpected.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                    e -> e.getValue() instanceof Object[] ? Arrays.asList((Object[]) e.getValue()) : e.getValue()));
        }
        Assertions.assertEquals(expected, values);
        if (verification != null) {
            verification.accept(values);
        }
    }

    private void dobuild(String pattern, Consumer<Dissect.Builder> configure, String value, Object expected)
            throws ProcessorException {
        dobuild(pattern, configure, value, expected, null);
    }

    private void dobuild(String pattern, String value, Object expected) throws ProcessorException {
        dobuild(pattern, b -> {
        }, value, expected);
    }

    @Test
    void appendTyped() throws ProcessorException {
        dobuild("%{a:int} %{+a}", b -> {
        }, "1 2", Map.of("a", new Object[] { 1, "2" }), values -> {
            @SuppressWarnings("unchecked") Map<String, Object> map = (Map<String, Object>) values;
            List<?> a = (List<?>) map.get("a");
            Assertions.assertInstanceOf(Integer.class, a.get(0));
            Assertions.assertInstanceOf(String.class, a.get(1));
        });
    }

    @Test
    void testBeans() throws IntrospectionException, ReflectiveOperationException {
        BeanChecks.beansCheck(logger, "loghub.processors.Dissect", BeanChecks.BeanInfo.build("pattern", String.class),
                BeanChecks.BeanInfo.build("appendSeparator", String.class),
                BeanChecks.BeanInfo.build("inPlace", Boolean.TYPE),
                BeanChecks.BeanInfo.build("destination", VariablePath.class),
                BeanChecks.BeanInfo.build("destinationTemplate", VarFormatter.class),
                BeanChecks.BeanInfo.build("field", VariablePath.class),
                BeanChecks.BeanInfo.build("fields", String[].class),
                BeanChecks.BeanInfo.build("path", VariablePath.class),
                BeanChecks.BeanInfo.build("if", Expression.class),
                BeanChecks.BeanInfo.build("success", Processor.class),
                BeanChecks.BeanInfo.build("failure", Processor.class),
                BeanChecks.BeanInfo.build("exception", Processor.class));
    }

}
