package loghub.configuration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import loghub.BuildableConnectionContext;
import loghub.ConnectionContext;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.IpConnectionContext;
import loghub.Lambda;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.RouteParser;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.types.NetworkMask;

public class TestExpressionParsing {

    private final EventsFactory factory = new EventsFactory();

    @BeforeAll
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.Expression", "loghub.VarFormatter", "loghub.groovy");
    }

    @Test
    public void testSimple() {
        Assertions.assertEquals("3", Tools.resolveExpression("1 + 2").toString());
    }

    @Test
    public void testOr() {
        Assertions.assertEquals("3", Tools.resolveExpression("1 .| 2").toString());
    }

    @Test
    public void testUnary() throws ProcessorException {
        Assertions.assertEquals(2, Tools.resolveExpression("-(-2)"));
        Assertions.assertEquals(-2, Tools.evalExpression(".~1"));
        Assertions.assertEquals(false, Tools.evalExpression("!1"));
    }

    @Test
    public void testSubExpression() {
        Assertions.assertEquals("12", Tools.resolveExpression("(1 + 2) * 4").toString());
    }

    @Test
    public void testNew() throws ProcessorException {
        Assertions.assertEquals(new Date(3), Tools.evalExpression("new java.util.Date(1+2)", factory.newEvent()));
        Assertions.assertEquals("", Tools.evalExpression("new java.lang.String()", factory.newEvent()));
        Event ev = factory.newEvent();
        RecognitionException ex = Assertions.assertThrows(RecognitionException.class, () -> Tools.evalExpression("new class.not.exist()", ev));
        Assertions.assertEquals("Unknown class: class.not.exist", ex.getMessage());
    }

    @Test
    public void testRoot() throws ProcessorException {
        Event ev =  factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), "1");
        Assertions.assertEquals(Map.copyOf(ev), Tools.evalExpression("[.]", ev.wrap(VariablePath.of("a"))));
        Assertions.assertEquals(Map.copyOf(ev), Tools.evalExpression("[.]", ev));
        Assertions.assertEquals(false, Tools.evalExpression("isEmpty([.])", ev));
    }

    @Test
    public void testFormatterSimple() throws ProcessorException {
        String format = "${#1%02d} ${#2%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        ev.put("b", 2);
        Assertions.assertEquals("01 02", Tools.evalExpression("\"" + format + "\"([a], [b])", ev));
    }

    @Test
    public void testFormatterFailed() {
        String format = "${#2%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        ProcessorException pe = Assertions.assertThrows(ProcessorException.class, () -> Tools.evalExpression("\"" + format + "\"([a])", ev));
        Assertions.assertTrue(pe.getMessage().contains("Not enough formatting arguments"));
    }

    @Test
    public void testFormatterTimestamp() throws ProcessorException {
        String format = "${#1%t<Europe/Paris>H}";
        Event ev =  factory.newEvent();
        ev.setTimestamp(new Date(0));
        Assertions.assertEquals("01", Tools.evalExpression("\"" + format + "\"([@timestamp])", ev));
    }

    @Test
    public void testFormatterContextPrincipal() throws ProcessorException {
        String format = "${#1%s}-${#2%tY}.${#2%tm}.${#2%td}";
        Event ev =  factory.newEvent(getConnexionContext(() -> "user"));
        ev.setTimestamp(new Date(0));
        Assertions.assertEquals("user-1970.01.01", Tools.evalExpression("\"" + format + "\" ([@context principal name], [@timestamp])", ev));
    }

    @Test
    public void testFormatterEvent() throws ProcessorException {
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        Assertions.assertEquals("1", Tools.evalExpression("\"${a}\"", ev));
    }

    @Test
    public void testEventPathVariation() {
        Event ev =  factory.newEvent();
        ev.put("a", Collections.singletonMap("b", "c"));
        Object[] tryExpression = new Object[] {
                "[a b]", "c",
                "[\"a\" \"b\"]", "c",
                "[a.b]", "c",
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testContextPattern() throws ProcessorException {
        Event ev =  factory.newEvent();
        String result = (String) Tools.evalExpression("\"${#1%s} ${#2%s}\"([@context], [@context principal]) ", ev);
        Assertions.assertTrue(Pattern.matches("loghub\\.events\\.LockedConnectionContext@[a-f0-9]+ loghub\\.([A-Za-z]+)ConnectionContext\\$EmptyPrincipal@[a-f0-9]+", result));
    }

    private void enumerateExpressions(Event ev, Object[] tryExpression) {
        Map<String, Object> tests = new LinkedHashMap<>(tryExpression.length / 2);
        for (int i = 0; i < tryExpression.length; ) {
            tests.put(tryExpression[i++].toString(), tryExpression[i++]);
        }
        tests.forEach((x, r) -> checkExpression(ev, x, r));
    }

    private void checkExpression(Event ev, String x, Object r) {
        try {
            Object o = Tools.evalExpression(x, ev);
            Assertions.assertEquals(r, o, x);
        } catch (IgnoredEventException e) {
            if (r != IgnoredEventException.class) {
                Assertions.fail(x + ", got an unexpected " + Helpers.resolveThrowableException(e));
            }
        } catch (ProcessorException e) {
            if (r != ProcessorException.class) {
                Assertions.fail(x + ", got an unexpected " + Helpers.resolveThrowableException(e));
            }
        }
    }

    @Test
    public void testInstanceOf() {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", "c");
        Object[] tryExpression = new Object[] {
                "1 instanceof java.lang.Integer", true,
                "1 !instanceof java.lang.Integer", false,
                "1 ! instanceof java.lang.Integer", false,
                "[a] instanceof java.lang.Integer", true,
                "[b] instanceof java.lang.Integer", false,
                "[a] !instanceof java.lang.Integer", false,
                "[b] !instanceof java.lang.Integer", true,
                "[b] ! instanceof java.lang.Integer", true,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testInstanceOfFailed() {
        RecognitionException ex = Assertions.assertThrows(RecognitionException.class, () -> Tools.evalExpression("1 instanceof not.a.class"));
        Assertions.assertEquals("Class not found: not.a.class", ex.getMessage());
    }

    @Test
    public void testOperators() throws UnknownHostException {
        Event ev = factory.newEvent();
        Object[] tryExpression = new Object[] {
                "'a' + 'b'", "ab",
                "1 instanceof java.lang.Integer", true,
                "1 !instanceof java.lang.Integer", false,
                "'a' in \"bac\"", true,
                "'d' in \"bac\"", false,
                "1 in list(1,2,3)", true,
                "4 in list(1,2,3)", false,
                "1 in set(1,2,3)", true,
                "4 in set(1,2,3)", false,
                "1 in (1,2,3)", true,
                "4 in (1,2,3)", false,
                "2 ** 3", 8,
                "2 ** (999999999 + 1)", Double.NaN,
                "2 * 2 ", 4,
                "2 + 4", 6,
                "true + false", true,
                "2 - 1", 1,
                "2 * 3", 6,
                "7 % 3", 1,
                "8 / 4", 2,
                "4 / 8", BigDecimal.valueOf(0.5),
                "1 << 2", 4,
                "4 >> 2", 1,
                "4 >>> 2", 1,
                "2 <= 2", true,
                "2 >= 1", true,
                "2 < 2", false,
                "2 > 2", false,
                "2 == 2", true,
                "2 != 2", false,
                "2 === 2", true,
                "2 !== 2", false,
                "2 <=> 2", 0,
                "2 <=> 1", 1,
                "1 <=> 2", -1,
                "3 .^ 2", 1,
                "2 .| 1", 3,
                "2 .& 1", 0,
                ".~ 1", -2,
                "+2", 2,
                "-2", -2,
                "!1", false,
                "!0", true,
                "!true", false,
                "!false", true,
                "(java.lang.Integer) 1", 1,
                "(java.net.InetAddress) \"127.0.0.1\"", InetAddress.getByName("127.0.0.1"),
                "(loghub.types.MacAddress)\"01:02:03:04:05:06\" == (loghub.types.MacAddress)\"01-02-03-04-05-06\"", true,
                "(loghub.types.MacAddress)\"01:02:03:04:05:06\" + (loghub.types.MacAddress)\"01-02-03-04-05-06\"", IgnoredEventException.class,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testInOperator() {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", 2);
        ev.put("c", 3);
        ev.put("d", List.of(1, 2, 3));
        checkExpression(ev, "1 in ([a], [b], [c])", true);
        checkExpression(ev, "1 in [d]", true);
        checkExpression(ev, "[a] in [d]", true);
    }

    @Test
    public void testErrorsConvert() {
        Event ev = factory.newEvent();
        ev.put("badIP", "255");
        ev.put("badNumber", "NONE");
        ProcessorException ex1 = Assertions.assertThrows(
                ProcessorException.class,
                () -> Tools.evalExpression("(java.net.InetAddress) [badIP]", ev)
        );
        Assertions.assertEquals("Failed expression (java.net.InetAddress) [badIP]: Unable to parse 255 as a java.net.InetAddress: Unknown host \"255\"", ex1.getMessage());
        ProcessorException ex2 = Assertions.assertThrows(
                ProcessorException.class,
                () -> Tools.evalExpression("(java.lang.Integer) [badNumber]", ev)
        );
        Assertions.assertEquals("Failed expression (java.lang.Integer) [badNumber]: Unable to parse NONE as a java.lang.Integer: For input string: \"NONE\"", ex2.getMessage());
        RecognitionException ex3 = Assertions.assertThrows(
                RecognitionException.class,
                () -> Tools.evalExpression("(java.net.InetAddress) \"255\"", ev)
        );
        Assertions.assertEquals("Unable to parse 255 as a java.net.InetAddress: Unknown host \"255\"", ex3.getMessage());
    }

    @Test
    public void dateCompare() {
        Instant now = Instant.now();
        Event ev = factory.newEvent();
        ev.put("a", now);
        ev.put("b", Date.from(now));
        ev.put("c", ZonedDateTime.ofInstant(now, ZoneId.systemDefault()));
        ev.put("d", 1);
        Object[] tryExpression = new Object[] {
                "[a] <=> [b]", 0,
                "[b] <=> [c]", 0,
                "[c] <=> [a]", 0,
                "[a] <=> [d]", IgnoredEventException.class,
                "[b] <=> [d]", IgnoredEventException.class,
                "[c] <=> [d]", IgnoredEventException.class,
                "[a] == [b]", true,
                "[b] == [c]", true,
                "[c] == [a]", true,
                "[a] != [b]", false,
                "[b] != [c]", false,
                "[c] != [a]", false,
                "[a] == [d]", false,
                "[b] == [d]", false,
                "[c] == [d]", false,
                "[d] == [a]", false,
                "[d] == [b]", false,
                "[d] == [c]", false,
                "[d] == [c]", false
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testOperatorsPath() {
        Event ev = factory.newEvent();
        ev.put("a", "");
        ev.put("b", 1);
        ev.put("c", "word");
        ev.put("d", 'w');
        Object[] tryExpression = new Object[] {
                "[a] == \"\"", true,
                "[b] == 1", true,
                "[c] == \"word\"", true,
                "[c] == \"\"", false,
                "[b] instanceof java.lang.Integer", true,
                "[b] !instanceof java.lang.Integer", false,
                "[d] in [c]", true,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testValueMissing() {
        Event ev = factory.newEvent();
        Object[] tryExpression = new Object[] {
                "null == [a b]", false,
                "[a b] == null", false,
                "[a b] != null", IgnoredEventException.class,
                "[a b] === null", false,
                "[a b] !== null", IgnoredEventException.class,
                "2 == [a b]", false,
                "[a b] == 2", false,
                "[a b] instanceof java.lang.Integer", false,
                "2 ** [a b]", IgnoredEventException.class,
                "[a b] ** 2", IgnoredEventException.class,
                "[a b] * 2 ", IgnoredEventException.class,
                "2 + [a b]", IgnoredEventException.class,
                "2 - [a b]", IgnoredEventException.class,
                "2 * [a b]", IgnoredEventException.class,
                "2 / [a b]", IgnoredEventException.class,
                "2 << [a b]", IgnoredEventException.class,
                "2 >> [a b]", IgnoredEventException.class,
                "2 >>> [a b]", IgnoredEventException.class,
                "2 <= [a b]", IgnoredEventException.class,
                "2 >= [a b]", IgnoredEventException.class,
                "2 < [a b]", IgnoredEventException.class,
                "2 > [a b]", IgnoredEventException.class,
                "2 <=> [a b]", IgnoredEventException.class,
                "[a b] <=> 2", IgnoredEventException.class,
                "2 .^ [a b]", IgnoredEventException.class,
                "2 .| [a b]", IgnoredEventException.class,
                "2 .& [a b]", IgnoredEventException.class,
                "true && [a b]", IgnoredEventException.class,
                "false || [a b]", IgnoredEventException.class,
                "2 in [a b]", IgnoredEventException.class,
                "[a] in list(1,2,3)", IgnoredEventException.class,
                ".~ [a b]", IgnoredEventException.class,
                "! [a b]", IgnoredEventException.class,
                "+ [a b]", IgnoredEventException.class,
                "- [a b]", IgnoredEventException.class,
                "[#a]", IgnoredEventException.class,
                "[#a] == null", false,
                "null == [#a]", false,
                "2 == [#a]", false,
                "[#a] == 2", false,
                "(java.lang.Double)[a]", IgnoredEventException.class,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testNullValue() {
        Event ev = factory.newEvent();
        ev.put("a", null);
        ev.putMeta("a", null);
        Object[] tryExpression = new Object[] {
                "null == [a]", true,
                "[a] == null", true,
                "[a] != null", false,
                "[a] === null", true,
                "2 == [a]", false,
                "[a] == 2", false,
                "[a] instanceof java.lang.Integer", false,
                "[a] !instanceof java.lang.Integer", true,
                "2 ** [a]", IgnoredEventException.class,
                "[a] ** 2", IgnoredEventException.class,
                "[a] * 2 ", IgnoredEventException.class,
                "2 + [a]", IgnoredEventException.class,
                "2 - [a]", IgnoredEventException.class,
                "2 * [a]", IgnoredEventException.class,
                "2 / [a]", IgnoredEventException.class,
                "2 << [a]", IgnoredEventException.class,
                "2 >> [a]", IgnoredEventException.class,
                "2 >>> [a]", IgnoredEventException.class,
                "2 <= [a]", IgnoredEventException.class,
                "2 >= [a]", IgnoredEventException.class,
                "2 < [a]", IgnoredEventException.class,
                "2 > [a]", IgnoredEventException.class,
                "2 <=> [a]", IgnoredEventException.class,
                "[a] <=> 2", IgnoredEventException.class,
                "2 .^ [a]", IgnoredEventException.class,
                "2 .| [a]", IgnoredEventException.class,
                "2 .& [a]", IgnoredEventException.class,
                "true && [a]", false,
                "false || [a]", false,
                "[a] && true", false,
                "[a] || false", false,
                "2 in [a]", false,
                "null in [a]", true,
                ".~ [a]", IgnoredEventException.class,
                "! [a]", true,
                "+ [a]", IgnoredEventException.class,
                "- [a]", IgnoredEventException.class,
                "null == [#a]", true,
                "[#a] == null", true,
                "2 == [#a]", false,
                "[#a] == 2", false,
                "(java.lang.Double)[a]", null,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testComparaison() {
        Event ev = factory.newEvent();
        Object[] tryExpression = new Object[] {
                "\"a\" <=> \"b\"", -1,
                "2 <=> 3", Integer.compare(2, 3),
                "3 <=> 2", Integer.compare(3, 2),
                "2 <=> 2", 0,
                "2 <=> null", IgnoredEventException.class,
                "2 < 3", true,
                "3 > 2", true,
                "2 <= 3", true,
                "3 <= 2", false,
                "2 <= 2", true,
                "2 >= 2", true,
                "2 <= \"b\"", true,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testTimestamp() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        Date ts = (Date) Tools.evalExpression("[@timestamp]", ev);
        Assertions.assertEquals(0L, ts.getTime());
    }

    @Test
    public void testTimestampCompare() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        Object o = Tools.evalExpression("[@timestamp] == [a]", ev);
        Assertions.assertEquals(true, o);
    }

    @Test
    public void testTimestampCompareMixedType() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        ev.put("b", new Date(0));
        Assertions.assertEquals(true, Tools.evalExpression("[a] == [b]", ev));
        Assertions.assertEquals(true, Tools.evalExpression("[b] == [a]", ev));
    }

    @Test
    public void testTimestampCompareTemporalAccessorType() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        ev.put("b", Instant.ofEpochMilli(0).atZone(ZoneId.systemDefault()));
        Assertions.assertEquals(true, Tools.evalExpression("[a] == [b]", ev));
        Assertions.assertEquals(true, Tools.evalExpression("[b] == [a]", ev));
    }

    @Test
    public void testNotTimestamp() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put(Event.TIMESTAMPKEY, 1);

        Object v = Tools.evalExpression("[ \"@timestamp\" ]", ev);
        Assertions.assertEquals(1, v);
    }

    @Test
    public void testMeta() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.putMeta("a", 1);
        Number i = (Number) Tools.evalExpression("[#a]", ev);
        Assertions.assertEquals(1, i.intValue());
    }

    @Test
    public void testArrayJoin() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", new int[] {4, 5, 6});
        @SuppressWarnings("unchecked")
        List<Object> i = (List<Object>) Tools.evalExpression("[a] + [b]", ev);
        Assertions.assertEquals(List.of(1, 2, 3, 4, 5, 6), i);
    }

    @Test
    public void testMap() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", 2);
        @SuppressWarnings("unchecked")
        Map<String, Object> i = (Map<String, Object>) Tools.evalExpression("{c: [a], d: [b]}", ev);
        Assertions.assertEquals(1, i.get("c"));
        Assertions.assertEquals(2, i.get("d"));
    }

    @Test
    public void testCollectionsJoin() throws ProcessorException {
        Set<?> i1 = (Set<?>) Tools.evalExpression("set(1, 2, 3) + list(4,5,6)", factory.newEvent());
        Assertions.assertEquals(Set.of(1, 2, 3, 4, 5, 6), i1);
        List<?> i2 = (List<?>) Tools.evalExpression("list(1, 2, 3) + set(4,5,6)", factory.newEvent());
        Assertions.assertEquals(List.of(1, 2, 3, 4, 5, 6), i2);
        List<?> i3 = (List<?>) Tools.evalExpression("list(1, 2, 3) + 4", factory.newEvent());
        Assertions.assertEquals(List.of(1, 2, 3, 4), i3);
    }

    @Test
    public void testArrayMixed() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(4, 5, 6));
        ev.put("c", new LinkedHashSet<>(List.of(7, 8, 9)));
        ev.put("d", List.of(1, 2, 3));
        ev.put("e", new int[] {1, 2, 3});
        ev.put("f", new ArrayList<>(List.of(1, 2, 3)));
        Assertions.assertEquals(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9), Tools.evalExpression("[a] + [b] + [c]", ev));
        Assertions.assertEquals(List.of(4, 5, 6, 7, 8, 9), Tools.evalExpression("[b] + [c]", ev));
        Assertions.assertEquals(Set.of(7, 8, 9, 4, 5, 6), Tools.evalExpression("[c] + [b]", ev));
        Assertions.assertEquals(List.of(1, 2, 3, 1, 2, 3), Tools.evalExpression("[a] + [d]", ev));
        Assertions.assertEquals(List.of(true, false, true), Tools.evalExpression("list(true, false) + set(true)", ev));
        Assertions.assertEquals(List.of('a', 'b', 'c'), Tools.evalExpression("list('a', 'b') + set('c')", ev));
        Assertions.assertEquals(List.of(1, 2, 3, 4), Tools.evalExpression("list(1, 2) + 3 + 4", ev));
        Assertions.assertEquals(Set.of(1, 2), Tools.evalExpression("set(1, 2) + 1 + 2", ev));
        Assertions.assertEquals(true, Tools.evalExpression("[a] == [d]", ev));
        Assertions.assertEquals(true, Tools.evalExpression("[a] == [e]", ev));
        Assertions.assertEquals(true, Tools.evalExpression("[a] == [f]", ev));
        Assertions.assertEquals(true, Tools.evalExpression("list((java.net.InetAddress)\"8.8.8.8\") == list((java.net.InetAddress)\"8.8.8.8\")", ev));
        Assertions.assertEquals(true, Tools.evalExpression("list('c') == list('c')", ev));
    }

    @Test
    public void testArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(1, 2, 3));
        ev.putMeta("a", null);
        Assertions.assertEquals(3, Tools.evalExpression("[a][2]", ev));
        Assertions.assertEquals(3, Tools.evalExpression("[b][2]", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[c][0]", ev));
        Assertions.assertNull(Tools.evalExpression("[#a][0]", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[#b][0]", ev));
    }

    @Test
    public void testArrayNegative() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(4, 5, 6));
        Assertions.assertEquals(3, Tools.evalExpression("[a][-1]", ev));
        Assertions.assertEquals(6, Tools.evalExpression("[b][-1]", ev));
    }

    @Test
    public void testArrayOutOfBound() {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[a][3]", ev));
    }

    @Test
    public void testList() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        Assertions.assertEquals(3, Tools.evalExpression("[a][2]", ev));
    }

    @Test
    public void testListOutOfBound() {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[a][3]", ev));
    }

    @Test
    public void testMissingArray() {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[b][3]", ev));
    }

    @Test
    public void testNotArray() {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assertions.assertThrows(ProcessorException.class, () -> Tools.evalExpression("[a][0]", ev));
    }

    @Test
    public void testPatternBoolean() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] ==~ /(a.)(.)/", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] ==~ \"\"\"(a.)(.)\"\"\"", ev));
    }

    @Test
    public void testPatternEmpty() throws ProcessorException {
        Set<Object> toTest = Set.of(NullOrMissingValue.NULL,
                Collections.emptySet(), Collections.emptyMap(), Collections.emptyList(),
                new Object[]{});
        Event ev = factory.newEvent();
        for (Object o : toTest) {
            ev.put("a", o);
            Boolean i = (Boolean) Tools.evalExpression("[a] ==~ /.*/", ev);
            Assertions.assertEquals(false, i);
        }
        ev.put("a", null);
        Boolean i = (Boolean) Tools.evalExpression("[a] ==~ /.*/", ev);
        Assertions.assertEquals(false, i);
    }

    @Test
    public void
    testPatternMissing() {
        Event ev = factory.newEvent();
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[a] ==~ /.*/", ev));
    }

    @Test
    public void testPatternBooleanEscaped() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "a.c\n");
        Assertions.assertEquals(true, Tools.evalExpression("[a] ==~ /a\\.c\\n/", ev));
    }

    @Test
    public void testPatternArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        String i = (String) Tools.evalExpression("([a] =~ /(a.)(.)/)[2]", ev);
        Assertions.assertEquals("c", i);
    }

    @Test
    public void testFailedPatternArray() {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("([a] =~ /d.*/)[2]", ev));
    }

    @Test
    public void testBadPattern() {
        Event ev = factory.newEvent();
        Assertions.assertThrows(RecognitionException.class, () -> Tools.evalExpression("[a] ==~ /*/", ev));
    }

    @Test
    public void testStringLitteral() {
        Assertions.assertEquals("loghub", Tools.resolveExpression("\"loghub\""));
        Assertions.assertEquals("\0", Tools.resolveExpression("\"\\0\""));
        Assertions.assertEquals("\1", Tools.resolveExpression("\"\\01\""));
        Assertions.assertEquals("\11", Tools.resolveExpression("\"\\11\""));
        Assertions.assertEquals("\111", Tools.resolveExpression("\"\\111\""));
        Assertions.assertEquals("a\111a", Tools.resolveExpression("\"a\\111a\""));
        Assertions.assertEquals("1\1", Tools.resolveExpression("\"1\\1\""));
        Assertions.assertEquals("\1", Tools.resolveExpression("\"\\u0001\""));
        Assertions.assertEquals("α", Tools.resolveExpression("\"\\u03B1\""));
        Assertions.assertEquals("Aαa", Tools.resolveExpression("\"A\\u03B1a\""));
        Assertions.assertEquals("\uD83C\uDF09", Tools.resolveExpression("\"\\ud83c\\udf09\""));
        Assertions.assertEquals("\n", Tools.resolveExpression("\"\\n\""));
        Assertions.assertEquals("\\n", Tools.resolveExpression("\"\\\\n\""));
    }

    @Test
    public void testCharacterLitteral() {
        Assertions.assertEquals('a', Tools.resolveExpression("'a'"));
        Assertions.assertEquals('\n', Tools.resolveExpression("'\n'"));
        Assertions.assertEquals('α', Tools.resolveExpression("'\u03B1'"));
    }

    @Test
    public void testIntegerLitteral() {
        Assertions.assertEquals(1, Tools.resolveExpression("1"));
        Assertions.assertEquals(100, Tools.resolveExpression("0100"));
        Assertions.assertEquals(Long.MAX_VALUE, Tools.resolveExpression("" + Long.MAX_VALUE));
        Assertions.assertEquals(new BigInteger("19223372036854775807"), Tools.resolveExpression("1" + Long.MAX_VALUE));
        Assertions.assertEquals(10, Tools.resolveExpression("0xa"));
        Assertions.assertEquals(511, Tools.resolveExpression("0o777"));
        Assertions.assertEquals(7, Tools.resolveExpression("0b111"));
    }

    @Test
    public void testNullLitteral() {
        Assertions.assertEquals(NullOrMissingValue.NULL, Tools.resolveExpression("null"));
    }

    @Test
    public void formatter1() throws ProcessorException {
        String expressionScript = "[a] == \"${a}\"";
        Event ev = factory.newEvent();
        ev.put("a", "a");
        Boolean b = (Boolean) Tools.evalExpression(expressionScript, ev);
        Assertions.assertTrue(b);
    }

    @Test
    public void formatter2() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        Object o = Tools.evalExpression("[a b] + \"${a.b}\"", ev);
        Assertions.assertEquals("11", o);
    }

    @Test
    public void testMultiFormat() throws ProcessorException {
        String format = "\"${a%s} ${b%02d}\"";
        Expression expression = Tools.parseExpression(format);
        Event ev = factory.newEvent();
        ev.put("a", "1");
        ev.put("b", 2);
        Object o = expression.eval(ev);
        Assertions.assertEquals("1 02", o);
    }

    @Test
    public void testDateDiff() {
        Instant now = Instant.now();
        Event ev = factory.newEvent();
        ev.put("a", now.minusMillis(1100));
        ev.put("b", now);
        ev.put("c", Date.from(now.minusMillis(1100)));
        ev.put("d", Date.from(now));
        List<String>  scripts = List.of("[a] - [b]", "[b] - [c]", "[c] - [d]", "[d] - [a]");
        Map<String, Double> results = new HashMap<>(scripts.size() * 2);
        scripts.forEach(s -> {
            try {
                Expression exp = Tools.parseExpression(s);
                 double f = (double) exp.eval(ev);
                results.put(s, f);
            } catch (ProcessorException e) {
                throw new RuntimeException(e);
            }
        });
        Assertions.assertEquals(-1.1, results.get("[a] - [b]"), 1e-3, "[a] - [b]");
        Assertions.assertEquals(1.1, results.get("[b] - [c]"), 1e-3, "[b] - [c]");
        Assertions.assertEquals(-1.1, results.get("[c] - [d]"), 1e-3, "[c] - [d]");
        Assertions.assertEquals(1.1, results.get("[d] - [a]"), 1e-3, "[d] - [a]");
        Expression exp = Tools.parseExpression("[c] - 1");
        Assertions.assertThrows(IgnoredEventException.class, () -> exp.eval(ev));
    }

    @Test
    public void testCollectionList() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", List.of(1, 2, 3));
        ev.put("c", Set.of(1, 2, 3));
        ev.put("d", new Integer[]{1, 2, 3});
        Assertions.assertEquals(List.of('a', "a", 1), Tools.evalExpression("list('a', \"a\", [a])", ev));
        Assertions.assertEquals(List.of(), Tools.evalExpression("list()", ev));
    }

    @Test
    public void testCollectionSet() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assertions.assertEquals(Set.of('a', "a", 1), Tools.evalExpression("set('a', 'a', \"a\", \"a\", [a])", ev));
        Assertions.assertEquals(Set.of(), Tools.evalExpression("set()", ev));
    }

    @Test
    public void testContext() throws ProcessorException, UnknownHostException {
        IpConnectionContext ipctx = new IpConnectionContext(new InetSocketAddress("127.0.0.1", 31712), new InetSocketAddress("www.google.com", 443));
        Principal p = () -> "user";
        ipctx.setPrincipal(p);
        Event ev = factory.newEvent(ipctx);
        Assertions.assertEquals(InetAddress.getByName("127.0.0.1"), Tools.evalExpression("[@context localAddress address]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[@context localAddress address] == \"127.0.0.1\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[@context localAddress port] == 31712", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[@context remoteAddress address] == \"www.google.com\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[@context remoteAddress port] == 443", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[@context principal name] == \"user\"", ev));

        // Checking both order of expression, change the way groovy handle it
        Assertions.assertEquals(true, Tools.evalExpression("\"user\" == [@context principal name]", ev));
    }

    @Test
    public void testContextNullOrMissing() throws ProcessorException {
        Event ev = factory.newEvent(getConnexionContext(() -> null));
        Assertions.assertTrue((boolean) Tools.evalExpression("isEmpty([@context principal name])", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("isEmpty([@context localAddress])", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[@context localAddress port] == 443", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[@context bad] == 443", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[@context remoteAddress path] == 443", ev));
    }

    @Test
    public void testDottedContext() throws ProcessorException {
        IpConnectionContext ipctxt = new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80));
        Principal p = () -> "user";
        ipctxt.setPrincipal(p);
        Event ev = factory.newEvent(ipctxt);
        Object value = Tools.evalExpression("[@context.principal.name] == \"user\"", ev);
        Assertions.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) Tools.evalExpression("[ @context.localAddress]", ev);
        Assertions.assertEquals(35710, localAddr.getPort());
    }

    private Object resolve(String function, String value) throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", value);
        return Tools.evalExpression(String.format("%s([a])", function), ev);
    }

    @Test
    public void testSplit() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "1 2 3");
        ev.put("b", "1/2/3");
        ev.put("c", null);
        Assertions.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(\" \", [a])", ev));
        Assertions.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(/ /, [a])", ev));
        Assertions.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(\"\"\"/\"\"\", [b])", ev));
        Assertions.assertNull(Tools.evalExpression("split(\" \", [c])", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("split(/ /, [d])", ev));
    }

    @Test
    public void testGsub() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "1 2 3");
        ev.put("b", "1/2/3");
        ev.put("c", null);
        Assertions.assertEquals("1-2-3", Tools.evalExpression("gsub([a], / /, \"-\")", ev));
        Assertions.assertEquals("1/2/3", Tools.evalExpression("gsub([b], / /, \"-\")", ev));
        Assertions.assertEquals("1-2-3", Tools.evalExpression("gsub([b], \"\"\"/\"\"\", \"-\")", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("gsub([a], \"\"\"/\"\"\", \"-\")", ev));
        Assertions.assertNull(Tools.evalExpression("gsub([c], / /, \"-\")", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("gsub([d], / /, \"-\")", ev));
    }

    @Test
    public void testJoin() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        ev.put("aL", new Long[]{1L, 2L, 3L});
        ev.put("af", new float[]{1.0f, 2.0f, 3.0f});
        ev.put("ac", new char[]{'1', '2', '3'});
        ev.put("b", new TreeSet<>(List.of(1, 2, 3, 1)));
        ev.put("c", ' ');
        ev.put("d", null);
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", set(1, 2, 3))", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", list(1, 2, 3))", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [a])", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [aL])", ev));
        Assertions.assertEquals("1.0 2.0 3.0", Tools.evalExpression("join(\" \", [af])", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [ac])", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [b])", ev));
        Assertions.assertNull(Tools.evalExpression("join(\" \", [d])", ev));
        Assertions.assertEquals("1 2 3", Tools.evalExpression("join(\" \", \"1 2 3\")", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("join(\" \", [e])", ev));
    }

    @Test
    public void testStringOperator() throws ProcessorException {
        Assertions.assertEquals("abc", resolve("trim", " abc "));
        Assertions.assertNull(resolve("trim", null));
        Assertions.assertEquals("Abc", resolve("capitalize", "abc"));
        Assertions.assertNull(resolve("capitalize", null));
        Assertions.assertEquals("abc", resolve("uncapitalize", "Abc"));
        Assertions.assertNull(resolve("uncapitalize", null));
        Assertions.assertEquals("abc", resolve("lowercase", "AbC"));
        Assertions.assertNull(resolve("lowercase", null));
        Assertions.assertEquals("ABC", resolve("uppercase", "abC"));
        Assertions.assertNull(resolve("uppercase", null));
        Assertions.assertEquals(false, resolve("isBlank", "abc"));
        Assertions.assertEquals(true, resolve("isBlank", ""));
        Assertions.assertEquals(true, resolve("isBlank", " "));
        Assertions.assertEquals(true, resolve("isBlank", null));
        Assertions.assertEquals("a\nb\nc\nd", resolve("normalize", "a\nb\r\nc\rd"));
        Assertions.assertNull(resolve("normalize", null));
        Event ev = factory.newEvent();
        ev.put("a", "1");
        ev.put("b", "2");
        Assertions.assertEquals("12", Tools.evalExpression("[a] + [b]", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isBlank([c])", ev));
    }

    @Test
    public void testCharOperator() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", ' ');
        ev.put("b", '1');
        ev.put("c", '2');
        Assertions.assertEquals("", Tools.evalExpression("trim([a])", ev));
        Assertions.assertEquals("12", Tools.evalExpression("[b] + [c]", ev));
    }

    @Test
    public void testComplexString() {
        String toEval = "a\"\\\t\n\r" + String.valueOf(Character.toChars(0x10000));
        StringBuilder buffer = new StringBuilder();
        toEval.chars().mapToObj(i -> {
            if (Character.isISOControl(i)) {
                return String.format("\\u%04X", i);
            } else if (i == '\\') {
                return "\\\\";
            } else if (i == '"') {
                return "\\\"";
            } else {
                return String.valueOf(Character.toChars(i));
            }
        }).forEach(buffer::append);
        String expression = String.format(Locale.US, "\"%s\"", buffer);
        Assertions.assertEquals(toEval, Tools.resolveExpression(expression));
    }

    @Test
    public void testNow() throws ProcessorException {
        Instant now = (Instant) Tools.evalExpression("now");
        Assertions.assertTrue(Math.abs(Instant.now().getEpochSecond() - now.getEpochSecond()) < 1);
    }

    @Test
    public void testHostname() throws ProcessorException, UnknownHostException {
        String hostname = InetAddress.getLocalHost().getHostName();
        Assertions.assertEquals(hostname, Tools.evalExpression("hostname"));
    }

    @Test
    public void testKeywordAsIdentifier() throws IOException {
        Pattern keywordidentifierPattern = Pattern.compile("'([a-zA-Z][a-zA-Z0-9$_]+)'=\\d+");
        Matcher m = keywordidentifierPattern.matcher("");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("target/generated-sources/antlr4/Route.tokens"), StandardCharsets.UTF_8))) {
            br.lines()
              .map(m::reset)
              .filter(Matcher::matches)
              .map(ma -> ma.group(1))
              .map(s -> "[" + s + "]")
              .forEach(Tools::parseExpression);
        }
    }

    @Test
    public void testIsEmptyTrue() throws ProcessorException {
        for (Object o : new Object[] {
                "",
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptySet(),
                null,
                new int[]{},
                new Integer[]{},
                new Object[]{}
        }) {
            Event ev = factory.newEvent();
            ev.put("a", o);
            Assertions.assertTrue((boolean) Tools.evalExpression("isEmpty([a])", ev));
            Assertions.assertFalse((boolean) Tools.evalExpression("! isEmpty([a])", ev));
        }
        Event ev = factory.newEvent();
        Assertions.assertTrue((boolean) Tools.evalExpression("isEmpty([.])", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("! isEmpty([.])", ev));
    }

    @Test
    public void testIsEmptyIgnore() {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("event", "type"), "debug");
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isEmpty([event start])", ev));
        Assertions.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isEmpty([a])", ev));
    }

    @Test
    public void testIsEmptyFalse() throws ProcessorException {
        for (Object o : new Object[] {
                " ",
                Collections.singletonMap("a", "b"),
                Collections.singletonList(""),
                Collections.singleton(""),
                new int[]{1},
                new Integer[]{1},
                new Object[]{""},
                0,
                1
        }) {
            Event ev = factory.newEvent();
            ev.put("a", o);
            Assertions.assertFalse((boolean) Tools.evalExpression("isEmpty([a])", ev));
            Assertions.assertTrue((boolean) Tools.evalExpression("! isEmpty([a])", ev));
        }
    }

    @Test
    public void testExistsTrue() throws ProcessorException {
        Event ev = factory.newEvent(new IpConnectionContext(InetSocketAddress.createUnresolved("localhost", 0), InetSocketAddress.createUnresolved("localhost", 0)));
        ev.putAtPath(VariablePath.of("event", "type"), "debug");
        ev.putAtPath(VariablePath.of("empty"), null);
        Assertions.assertTrue((Boolean) Tools.evalExpression("[event type] == *", ev));
        Assertions.assertTrue((Boolean) Tools.evalExpression("[empty] == *", ev));
        Assertions.assertTrue((Boolean) Tools.evalExpression("[@context localAddress port] == *", ev));
    }

    @Test
    public void testExistsFalse() throws ProcessorException {
        Event ev = factory.newEvent();
        Assertions.assertFalse((Boolean) Tools.evalExpression("[a] == *", ev));
        Assertions.assertFalse((Boolean) Tools.evalExpression("[@context localAddress port] == *", ev));
    }

    @Test
    public void optimizeVarFormat() {
        String lambda = "\">${#1%s}<\"(1)";
        Assertions.assertEquals(">1<", ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void optimizeVarFormatWithEvent() {
        String lambda = "\"${%j}\"";
        Assertions.assertEquals(Expression.class, ConfigurationTools.unWrap(lambda, RouteParser::expression).getClass());
    }

    @Test
    public void optimizeConstantNumeric() {
        String lambda = "1";
        Assertions.assertEquals(Integer.valueOf(1), ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void optimizeConstantString() {
        String lambda = "\"1\"";
        Assertions.assertEquals("1", ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void parseLambda() throws ProcessorException {
        String lambda = "x -> x + 1";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assertions.assertEquals(2, l.expression().eval(null, 1));
    }

    @Test
    public void parseLambdaConstant() throws ProcessorException {
        String lambda = "x -> 1";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assertions.assertEquals(1, l.expression().eval(null, 1));
    }

    @Test
    public void parseLambdaFormatter() throws ProcessorException {
        String lambda = "x -> \">${#1%s}<\"(x)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assertions.assertEquals(">1<", l.expression().eval(null, 1));
    }

    @Test
    public void parseLambdaConstantFormatter() throws ProcessorException {
        String lambda = "x -> \">${#1%s}<\"(1)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assertions.assertEquals(">1<", l.expression().eval(null, null));
    }

    @Test
    public void parseLambdaIsEmpty() throws ProcessorException {
        String lambda = "x -> isEmpty(x)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assertions.assertEquals(true, l.expression().eval(null, List.of()));
    }

    @Test
    public void parseBadLambda() {
        String lambda = "x -> y + 1";
        ConfigException ex = Assertions.assertThrows(ConfigException.class, () -> ConfigurationTools.unWrap(lambda, RouteParser::lambda));
        Assertions.assertEquals("Invalid lambda definition", ex.getMessage());
    }

    @Test
    public void testIp() throws ProcessorException, UnknownHostException {
        Event ev = factory.newEvent();
        ev.put("a", InetAddress.getByName("192.168.1.1"));
        ev.put("b", InetAddress.getByName("www.google.com"));
        ev.put("c", InetAddress.getByName("127.0.0.1"));
        ev.put("d", InetAddress.getByName("::1"));
        // Simulate a host with both IPv4 and IPv6, not always true
        ev.put("e", new InetAddress[] {InetAddress.getByName("127.0.0.1"), InetAddress.getByName("::1")});

        Assertions.assertTrue((boolean) Tools.evalExpression("[a] == \"192.168.1.1\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] == \"/192.168.1.1\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[b] == \"www.google.com\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[c] == \"localhost\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[e] == \"127.0.0.1\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[e] == \"::1\"", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"192.168.1.1\" == [a]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"/192.168.1.1\" == [a]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"www.google.com\" == [b]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"localhost\" == [c]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"127.0.0.1\" == [e]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("\"::1\" == [e]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("isIP([a])", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("isIP(\"192.168.1.1\")", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("isIP(\"www.google.com\")", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("isIP(\"::1\")", ev));
    }

    @Test
    public void testBoolean() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", true);
        ev.put("b", 1);
        ev.put("c", 1.0);
        ev.put("d", false);
        ev.put("e", 0);
        ev.put("f", 0.0);
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] && [b]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] && [c]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[b] && [c]", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[a] && [d]", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[a] && [e]", ev));
        Assertions.assertFalse((boolean) Tools.evalExpression("[b] && [f]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] || [d]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[a] || [e]", ev));
        Assertions.assertTrue((boolean) Tools.evalExpression("[b] || [f]", ev));
    }

    @Test
    public void constantOptimization() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(1);
        ExpressionBuilder b2 = ExpressionBuilder.of(2);
        ExpressionBuilder b3 = ExpressionBuilder.of(b1, "+", b2);
        Assertions.assertEquals(3, b3.build("1+2").eval());
    }

    @Test
    public void staticFormat() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(new VarFormatter("1"));
        Assertions.assertEquals("1", b1.build("1").eval());
    }

    @Test
    public void lambdaOptimization() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(1);
        ExpressionBuilder b2 = ExpressionBuilder.of(b1, o -> Expression.instanceOf(false, o, Integer.class));
        ExpressionBuilder b3 = ExpressionBuilder.of(b1, o -> Expression.instanceOf(true, o, Integer.class));
        Assertions.assertEquals(true, b2.build("1 instanceof Integer").eval());
        Assertions.assertEquals(false, b3.build("1 ! instanceof Integer").eval());
    }

    @Test
    public void testWrappedDeepCopy() {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        ev.putAtPath(VariablePath.of("a", "c"), null);
        List<Object> d = new ArrayList<>(2);
        d.add(NullOrMissingValue.NULL);
        d.add(null);
        ev.putAtPath(VariablePath.of("a", "d"), d);
        Event wrapped = ev.wrap(VariablePath.of("a"));
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) Expression.deepCopy(wrapped);
        Assertions.assertEquals(1, map.get("b"));
        Assertions.assertEquals(NullOrMissingValue.NULL, map.get("c"));
        Assertions.assertEquals(List.of(NullOrMissingValue.NULL, NullOrMissingValue.NULL), map.get("d"));
    }

    @Test
    @DisplayName("Test flatten")
    public void testFlatten() throws ProcessorException {
        Event ev = factory.newTestEvent();
        Assertions.assertEquals(Set.of(1, 2), Tools.evalExpression("flatten(set(list(1), 2, list(1)))", ev));
    }

    <T> ConnectionContext<T> getConnexionContext(Principal principal) {
        BuildableConnectionContext<T> newCtxt = new BuildableConnectionContext<>() {
            @Override
            public T getLocalAddress() {
                return null;
            }

            @Override
            public T getRemoteAddress() {
                return null;
            }
        };
        newCtxt.setPrincipal(principal);
        return newCtxt;
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("ipCases")
    public void testParsingIp(String expression, Object expected) {
        Event ev = factory.newEvent();
        checkExpression(ev, expression, expected);
    }

    private static Stream<Arguments> ipCases() throws UnknownHostException {
        return Stream.of(
                Arguments.of("127.0.0.1", InetAddress.getByName("127.0.0.1")),
                Arguments.of("127.0.0.1/8", new NetworkMask(InetAddress.getByName("127.0.0.1"), (short) 8)),
                Arguments.of("2001:db8::ff00:42:8329", InetAddress.getByName("2001:db8::ff00:42:8329")),
                Arguments.of("::1", InetAddress.getByName("::1")),
                Arguments.of("::", InetAddress.getByName("::")),
                Arguments.of("1:2:3:4:5:6:7:8", InetAddress.getByName("1:2:3:4:5:6:7:8")),
                Arguments.of("1:2:3:4:5:6:7::", InetAddress.getByName("1:2:3:4:5:6:7::")),
                Arguments.of("::ffff:10.0.0.1", InetAddress.getByName("10.0.0.1")),
                Arguments.of("127.0.0.1 == \"127.0.0.1\"", true),
                Arguments.of("127.0.0.1 in 127.0.0.1/8", true),
                Arguments.of("192.168.1.2 in 127.0.0.1/8", false),
                Arguments.of("127.0.0.1 !in 127.0.0.1/8", false),
                Arguments.of("192.168.1.2 !in 127.0.0.1/8", true),
                Arguments.of("::1 in ::1/128", true),
                Arguments.of("2001:db8::1 in 2001:db8::/32", true),
                Arguments.of("2001:db8::1 in 2001:db9::/32", false),
                Arguments.of("127.0.0.1 in ::1/128", false),
                Arguments.of("::1 in 127.0.0.1/8", false),
                Arguments.of("\"not-an-ip\" in 127.0.0.1/8", false),
                Arguments.of("127.0.0.1/8 in 127.0.0.1", false)
        );
    }

    @ParameterizedTest(name = "[{index}] {0} == {1} -> {2}")
    @MethodSource("ipEqualityCases")
    public void testparsingIpEquality(String maskA, String maskB, boolean expected) throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", NetworkMask.of(maskA));
        ev.put("b", NetworkMask.of(maskB));
        Assertions.assertEquals(expected, Tools.evalExpression("[a] == [b]", ev));
    }

    private static Stream<Arguments> ipEqualityCases() {
        return Stream.of(
                Arguments.of("127.0.0.1/8", "127.0.0.1/8", true),
                Arguments.of("127.0.0.1/8", "127.0.0.2/8", true),
                Arguments.of("127.0.0.1/8", "127.0.0.1/16", false)
        );
    }

    @ParameterizedTest(name = "[{index}] {0} -> {1}")
    @MethodSource("ipToStringCases")
    public void testParsingIpToString(String mask, String expected) throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", NetworkMask.of(mask));
        Assertions.assertEquals(expected, Tools.evalExpression("\"${a}\"", ev));
    }

    private static Stream<Arguments> ipToStringCases() {
        return Stream.of(
                Arguments.of("127.0.0.1/8", "127.0.0.0/8"),
                Arguments.of("192.168.1.1/24", "192.168.1.0/24")
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("ipInvalidMaskCases")
    public void testParsingIpInvalidMask(String expression) {
        RecognitionException re = Assertions.assertThrows(RecognitionException.class, () -> Tools.evalExpression(expression));
        Assertions.assertEquals("Invalid CIDR netmask " + expression, re.getMessage());
    }

    private static Stream<Arguments> ipInvalidMaskCases() {
        return Stream.of(
                Arguments.of("127.0.0.1/33"),
                Arguments.of("::1/129"),
                Arguments.of("::1/65536")
        );
    }

}
