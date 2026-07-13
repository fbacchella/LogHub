package loghub.processors;

import java.beans.IntrospectionException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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

    @ParameterizedTest
    @MethodSource("patterns")
    void testPatterns(String pattern, String value, Map<String, Object> expected) throws ProcessorException {
        dobuild(pattern, value, expected);
    }

    static Stream<Arguments> patterns() {
        return Stream.of(
                Arguments.of("%{a} %{b} %{c}", "1 2 3",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3"))),
                // Right padding modifier ->
                Arguments.of("%{a->} %{b} %{c}", "1   2 3",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3"))),
                Arguments.of("%{a->},%{b},%{c}", "1,,,,2,3",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3"))),
                Arguments.of("%{a->},:%{b},%{c}", "1,:,:,:,:2,3",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3"))),
                Arguments.of("%{->},%{b},%{c}", "1,,,,2,3", Map.ofEntries(Map.entry("b", "2"), Map.entry("c", "3"))),
                // Append modifier +
                Arguments.of("%{a} %{+a} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "2", "3" }))),
                // Append modifier with order + with /n
                Arguments.of("%{a} %{+a/2} %{+a/1}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "3", "2" }))),
                // Mixed append: add(foundValue) and set(key.appendModifier, foundValue)
                // first +a adds to index 0, then +a/1 sets index 1, then +a adds to index 1 (overwriting!)
                Arguments.of("%{+a} %{+a/1} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "1", "3", null }))),
                // Another mixed case: +a/2 sets index 2, +a adds to index 0, +a adds to index 1
                Arguments.of("%{+a/2} %{+a} %{+a}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "2", "3", "1" }))),
                // Named skip key ?
                Arguments.of("%{a} %{?skipme} %{c}", "1 2 3", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "3"))),
                Arguments.of("%{a} %{?skipme} %{?skipme} %{c}", "1 2 3 4", Map.ofEntries(Map.entry("a", "1"), Map.entry("c", "4"))),
                // Reference keys * and &
                Arguments.of("%{*a} %{b} %{&a}", "c 1 2", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2"))),
                Arguments.of("%{&a} %{b} %{*a}", "2 1 c", Map.ofEntries(Map.entry("b", "1"), Map.entry("c", "2"))),
                // Remaining match
                Arguments.of("%{a} %{b},%{c}", "1 2,3  4",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3  4"))),
                // Consecutive repeating delimiters
                Arguments.of("%{a},%{b},%{c},%{d}", "1,,,4",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", ""), Map.entry("d", "4"))),
                Arguments.of("%{a},%{b},%{c},%{d},%{e},%{f},%{g}", "1,,2,,,,3",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", ""), Map.entry("c", "2"), Map.entry("d", ""),
                                Map.entry("e", ""), Map.entry("f", ""), Map.entry("g", "3"))),
                // More tests
                Arguments.of("0%{a} %{b} %{c}4", "01 2 34",
                        Map.ofEntries(Map.entry("a", "1"), Map.entry("b", "2"), Map.entry("c", "3"))),
                Arguments.of("%{+a/2} %{+a/1} %{+a/0}", "1 2 3", Map.ofEntries(Map.entry("a", new Object[] { "3", "2", "1" }))),
                // Pattern starting/ending with fixed string
                Arguments.of("start %{a} end", "start 1 end", Map.ofEntries(Map.entry("a", "1"))),
                // Complex padding case: padding at the end of the chain
                Arguments.of("%{a->} ", "1   ", Map.ofEntries(Map.entry("a", "1"))),
                Arguments.of("%{a.b} %{c.d}", "1 2", Map.ofEntries(Map.entry("a.b", "1"), Map.entry("c.d", "2"))),
                // Explicit class
                Arguments.of("%{a:java.lang.Integer} %{b:java.lang.Long}", "1 2", Map.ofEntries(Map.entry("a", 1), Map.entry("b", 2L)))
        );
    }

    @Test
    void testWithSeparator() throws ProcessorException {
        dobuild("%{a} %{+a} %{+a}", b -> b.setAppendSeparator(", "), "1 2 3", Map.ofEntries(Map.entry("a", "1, 2, 3")));
    }

    @Test
    void testPostfix() throws ProcessorException {
        // Postfix pattern
        dobuild("%{timestamp} %{+timestamp} %{+timestamp} %{logsource} %{program}[%{pid}]: %{message}",
                b -> b.setAppendSeparator(" "),
                "Mar 16 00:01:25 example postfix/smtpd[1713]: connect from example.com[192.100.1.3]",
                Map.ofEntries(Map.entry("timestamp", "Mar 16 00:01:25"), Map.entry("pid", "1713"),
                        Map.entry("program", "postfix/smtpd"), Map.entry("logsource", "example"),
                        Map.entry("message", "connect from example.com[192.100.1.3]")));
    }

    @ParameterizedTest
    @MethodSource("invalidPatterns")
    void testInvalidPatterns(Class<? extends Exception> eClass, String pattern, String expectedMessage) {
        checkException(eClass, pattern, expectedMessage);
    }

    static Stream<Arguments> invalidPatterns() {
        return Stream.of(
                Arguments.of(IllegalArgumentException.class, "%{/2}", "Append order defined, without append modifier"),
                Arguments.of(IllegalArgumentException.class, "%{a:nothing}", "Unknown conversion type: nothing"),
                Arguments.of(IllegalArgumentException.class, "%{a} %{+a/2}", "Appender modifier out of range for key \"a\""),
                Arguments.of(IllegalArgumentException.class, "%{a} %{*a}", "Reference key \"a\" must be specified as pair"),
                Arguments.of(IllegalArgumentException.class, "%{*a} %{*a}", "Needs a reference name and a reference value for key \"a\""),
                Arguments.of(IllegalArgumentException.class, "%{&a} %{&a}", "Needs a reference name and a reference value for key \"a\""),
                Arguments.of(IllegalArgumentException.class, "%{*a:int} %{&a}", "A type conversion can't be applied to a reference name")
        );
    }

    @ParameterizedTest
    @MethodSource("failedMatches")
    void testFailedMatches(String pattern, String value) throws ProcessorException {
        dobuild(pattern, value, FieldsProcessor.RUNSTATUS.FAILED);
    }

    static Stream<Arguments> failedMatches() {
        return Stream.of(
                Arguments.of("%{a:int}", "notanint"),
                Arguments.of("prefix%{a} %{b}", "1 2"),
                Arguments.of("%{a} %{b}suffix", "1 2"),
                Arguments.of("%{a} %{b} %{c}", "1 2"),
                Arguments.of("prefix%{a}", "1"),
                Arguments.of("%{a},%{b}", "1 2")
        );
    }

    @Test
    void testTypePatternRestriction() throws ProcessorException {
        // %{a:123} -> '123' n'est pas un type valide (doit être minuscule ou identifiant Java)
        // Donc 'a:123' est pris comme nom de clé.
        // Mais par défaut, Dissect n'accepte pas ':' dans les noms de clés (voir keyPattern)
        // sauf si c'est le séparateur pour le type.
        // Donc %{a:123} ne matche pas du tout le keyPattern.
        // Résultat: le processeur n'extrait rien, et dobuild échoue car la valeur ne matche pas le pattern.
        dobuild("%{a:123}", "val", FieldsProcessor.RUNSTATUS.FAILED);

        // Cas valides pour le type
        dobuild("%{a:int}", "1", Map.of("a", 1));
        dobuild("%{a:Integer}", "1", Map.of("a", 1));
        dobuild("%{a:java.lang.Integer}", "1", Map.of("a", 1));

        // Cas invalide (minuscules mais inconnu)
        checkException(IllegalArgumentException.class, "%{a:inconnu}", "Unknown conversion type: inconnu");
    }

    @Test
    void testExplicitClass() throws ProcessorException {
        dobuild("%{a:java.lang.Integer}", "1", Map.of("a", 1));
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
    void testIp() throws ProcessorException, UnknownHostException {
        dobuild("%{ipv4:ip} %{ipv6:ip}", "127.0.0.1 ::1",
                Map.ofEntries(Map.entry("ipv4", InetAddress.getByName("127.0.0.1")),
                        Map.entry("ipv6", InetAddress.getByName("::1"))));
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
