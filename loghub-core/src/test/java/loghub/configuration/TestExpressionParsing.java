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

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class TestExpressionParsing {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.Expression", "loghub.VarFormatter");
    }

    @Test
    public void testSimple() {
        Assert.assertEquals("3", Tools.resolveExpression("1 + 2").toString());
    }

    @Test
    public void testOr() {
        Assert.assertEquals("3", Tools.resolveExpression("1 .| 2").toString());
    }

    @Test
    public void testUnary() throws ProcessorException {
        Assert.assertEquals(2, Tools.resolveExpression("-(-2)"));
        Assert.assertEquals(-2, Tools.evalExpression(".~1"));
        Assert.assertEquals(false, Tools.evalExpression("!1"));
    }

    @Test
    public void testSubExpression() {
        Assert.assertEquals("12", Tools.resolveExpression("(1 + 2) * 4").toString());
    }

    @Test
    public void testNew() throws ProcessorException {
        Assert.assertEquals(new Date(3), Tools.evalExpression("new java.util.Date(1+2)", factory.newEvent()));
        Assert.assertEquals("", Tools.evalExpression("new java.lang.String()", factory.newEvent()));
        Event ev = factory.newEvent();
        RecognitionException ex = Assert.assertThrows(RecognitionException.class, () -> Tools.evalExpression("new class.not.exist()", ev));
        Assert.assertEquals("Unknown class: class.not.exist", ex.getMessage());
    }

    @Test
    public void testRoot() throws ProcessorException {
        Event ev =  factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), "1");
        Assert.assertEquals(Map.copyOf(ev), Tools.evalExpression("[.]", ev.wrap(VariablePath.of("a"))));
        Assert.assertEquals(Map.copyOf(ev), Tools.evalExpression("[.]", ev));
        Assert.assertEquals(false, Tools.evalExpression("isEmpty([.])", ev));
    }

    @Test
    public void testFormatterSimple() throws ProcessorException {
        String format = "${#1%02d} ${#2%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        ev.put("b", 2);
        Assert.assertEquals("01 02", Tools.evalExpression("\"" + format + "\"([a], [b])", ev));
    }

    @Test
    public void testFormatterFailed() {
        String format = "${#2%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        ProcessorException pe = Assert.assertThrows(ProcessorException.class, () -> Tools.evalExpression("\"" + format + "\"([a])", ev));
        Assert.assertTrue(pe.getMessage().contains("index out of range"));
    }

    @Test
    public void testFormatterTimestamp() throws ProcessorException {
        String format = "${#1%t<Europe/Paris>H}";
        Event ev =  factory.newEvent();
        ev.setTimestamp(new Date(0));
        Assert.assertEquals("01", Tools.evalExpression("\"" + format + "\"([@timestamp])", ev));
    }

    @Test
    public void testFormatterContextPrincipal() throws ProcessorException {
        String format = "${#1%s}-${#2%tY}.${#2%tm}.${#2%td}";
        Event ev =  factory.newEvent(new ConnectionContext<>() {
            @Override
            public Object getLocalAddress() {
                return null;
            }

            @Override
            public Object getRemoteAddress() {
                return null;
            }
        });
        ev.setTimestamp(new Date(0));
        Principal p = () -> "user";
        ev.getConnectionContext().setPrincipal(p);
        Assert.assertEquals("user-1970.01.01", Tools.evalExpression("\"" + format + "\" ([@context principal name], [@timestamp])", ev));
    }

    @Test
    public void testFormatterEvent() throws ProcessorException {
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals("1", Tools.evalExpression("\"${a}\"", ev));
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
        Assert.assertTrue(Pattern.matches("loghub.ConnectionContext\\$1@[a-f0-9]+ loghub.ConnectionContext\\$EmptyPrincipal@[a-f0-9]+", result));
    }

    private void enumerateExpressions(Event ev, Object[] tryExpression) {
        Map<String, Object> tests = new LinkedHashMap<>(tryExpression.length / 2);
        for (int i = 0; i < tryExpression.length; ) {
            tests.put(tryExpression[i++].toString(), tryExpression[i++]);
        }
        tests.forEach((x, r) -> {
            try {
                Object o = Tools.evalExpression(x, ev);
                Assert.assertEquals(x, r, o);
            } catch (IgnoredEventException e) {
                if (r != IgnoredEventException.class) {
                    Assert.fail(x + ", got an unexpected " + Helpers.resolveThrowableException(e));
                }
            } catch (ProcessorException e) {
                if (r != ProcessorException.class) {
                    Assert.fail(x + ", got an unexpected " + Helpers.resolveThrowableException(e));
                }
            }
        });
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
        RecognitionException ex = Assert.assertThrows(RecognitionException.class, () -> Tools.evalExpression("1 instanceof not.a.class"));
        Assert.assertEquals("Class not found: not.a.class", ex.getMessage());
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
                "2 ** 3", 8,
                "2 ** (999999999 + 1)", Double.NaN,
                "2 * 2 ", 4,
                "2 + 4", 6,
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
        };
        enumerateExpressions(ev, tryExpression);
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
        Assert.assertEquals(0L, ts.getTime());
    }

    @Test
    public void testTimestampCompare() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        Object o = Tools.evalExpression("[@timestamp] == [a]", ev);
        Assert.assertEquals(true, o);
    }

    @Test
    public void testTimestampCompareMixedType() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        ev.put("b", new Date(0));
        Assert.assertEquals(true, Tools.evalExpression("[a] == [b]", ev));
        Assert.assertEquals(true, Tools.evalExpression("[b] == [a]", ev));
    }

    @Test
    public void testTimestampCompareTemporalAccessorType() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        ev.put("b", Instant.ofEpochMilli(0).atZone(ZoneId.systemDefault()));
        Assert.assertEquals(true, Tools.evalExpression("[a] == [b]", ev));
        Assert.assertEquals(true, Tools.evalExpression("[b] == [a]", ev));
    }

    @Test
    public void testNotTimestamp() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put(Event.TIMESTAMPKEY, 1);

        Object v = Tools.evalExpression("[ \"@timestamp\" ]", ev);
        Assert.assertEquals(1, v);
    }

    @Test
    public void testMeta() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.putMeta("a", 1);
        Number i = (Number) Tools.evalExpression("[#a]", ev);
        Assert.assertEquals(1, i.intValue());
    }

    @Test
    public void testArrayJoin() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", new Integer[] {4, 5, 6});
        Object[] i = (Object[]) Tools.evalExpression("[a] + [b]", ev);
        Assert.assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6}, i);
    }

    @Test
    public void testCollectionsJoin() throws ProcessorException {
        Set<?> i1 = (Set<?>) Tools.evalExpression("set(1, 2, 3) + list(4,5,6)", factory.newEvent());
        Assert.assertEquals(Set.of(1, 2, 3, 4, 5, 6), i1);
        List<?> i2 = (List<?>) Tools.evalExpression("list(1, 2, 3) + set(4,5,6)", factory.newEvent());
        Assert.assertEquals(List.of(1, 2, 3, 4, 5, 6), i2);
        List<?> i3 = (List<?>) Tools.evalExpression("list(1, 2, 3) + 4", factory.newEvent());
        Assert.assertEquals(List.of(1, 2, 3, 4), i3);
    }

    @Test
    public void testArrayMixed() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(4, 5, 6));
        ev.put("c", new LinkedHashSet<>(List.of(7, 8, 9)));
        Object[] i = (Object[]) Tools.evalExpression("[a] + [b] + [c]", ev);
        Assert.assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, i);
        // Check by starting with an immutable list and no public constructor
        Object i2 = Tools.evalExpression("[b] + [c]", ev);
        Assert.assertEquals(List.of(4, 5, 6, 7, 8, 9), i2);
    }

    @Test
    public void testArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(1, 2, 3));
        ev.putMeta("a", null);
        Assert.assertEquals(3, Tools.evalExpression("[a][2]", ev));
        Assert.assertEquals(3, Tools.evalExpression("[b][2]", ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[c][0]", ev));
        Assert.assertNull(Tools.evalExpression("[#a][0]", ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[#b][0]", ev));
    }

    @Test
    public void testArrayNegative() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(4, 5, 6));
        Assert.assertEquals(3, Tools.evalExpression("[a][-1]", ev));
        Assert.assertEquals(6, Tools.evalExpression("[b][-1]", ev));
    }

    @Test(expected=IgnoredEventException.class)
    public void testArrayOutOfBound() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        Tools.evalExpression("[a][3]", ev);
    }

    @Test
    public void testList() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        Assert.assertEquals(3, Tools.evalExpression("[a][2]", ev));
    }

    @Test(expected=IgnoredEventException.class)
    public void testListOutOfBound() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        Tools.evalExpression("[a][3]", ev);
    }

    @Test(expected=IgnoredEventException.class)
    public void testMissingArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        Tools.evalExpression("[b][3]", ev);
    }

    @Test(expected=ProcessorException.class)
    public void testNotArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Tools.evalExpression("[a][0]", ev);
    }

    @Test
    public void testPatternBoolean() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        Assert.assertTrue((boolean) Tools.evalExpression("[a] ==~ /(a.)(.)/",ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[a] ==~ \"\"\"(a.)(.)\"\"\"",ev));
    }

    @Test
    public void testPatternEmpty() throws ProcessorException {
        Set<Object> toTest = Set.of(NullOrMissingValue.NULL,
                Collections.emptySet(), Collections.emptyMap(), Collections.emptyList(),
                new Object[]{});
        Event ev = factory.newEvent();
        for(Object o: toTest) {
            ev.put("a", o);
            Boolean i = (Boolean) Tools.evalExpression("[a] ==~ /.*/",ev);
            Assert.assertEquals(false, i);
        }
        ev.put("a", null);
        Boolean i = (Boolean) Tools.evalExpression("[a] ==~ /.*/",ev);
        Assert.assertEquals(false, i);
    }

    @Test
    public void
    testPatternMissing() {
        Event ev = factory.newEvent();
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("[a] ==~ /.*/",ev));
    }

    @Test
    public void testPatternBooleanEscaped() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "a.c\n");
        Assert.assertEquals(true, Tools.evalExpression("[a] ==~ /a\\.c\\n/",ev));
    }

    @Test
    public void testPatternArray() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        String i = (String) Tools.evalExpression("([a] =~ /(a.)(.)/)[2]",ev);
        Assert.assertEquals("c", i);
    }

    @Test
    public void testFailedPatternArray() {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("([a] =~ /d.*/)[2]",ev));
    }

    @Test(expected = RecognitionException.class)
    public void testBadPattern() throws ProcessorException {
        Event ev = factory.newEvent();
        Tools.evalExpression("[a] ==~ /*/",ev);
    }

    @Test
    public void testStringLitteral() {
        Assert.assertEquals("loghub", Tools.resolveExpression("\"loghub\""));
        Assert.assertEquals("\0", Tools.resolveExpression("\"\\0\""));
        Assert.assertEquals("\1", Tools.resolveExpression("\"\\01\""));
        Assert.assertEquals("\11", Tools.resolveExpression("\"\\11\""));
        Assert.assertEquals("\111", Tools.resolveExpression("\"\\111\""));
        Assert.assertEquals("a\111a", Tools.resolveExpression("\"a\\111a\""));
        Assert.assertEquals("1\1", Tools.resolveExpression("\"1\\1\""));
        Assert.assertEquals("\1", Tools.resolveExpression("\"\\u0001\""));
        Assert.assertEquals("α", Tools.resolveExpression("\"\\u03B1\""));
        Assert.assertEquals("Aαa", Tools.resolveExpression("\"A\\u03B1a\""));
        Assert.assertEquals("\uD83C\uDF09", Tools.resolveExpression("\"\\ud83c\\udf09\""));
        Assert.assertEquals("\n", Tools.resolveExpression("\"\\n\""));
        Assert.assertEquals("\\n", Tools.resolveExpression("\"\\\\n\""));
    }

    @Test
    public void testCharacterLitteral() {
        Assert.assertEquals('a', Tools.resolveExpression("'a'"));
        Assert.assertEquals('\n', Tools.resolveExpression("'\n'"));
        Assert.assertEquals('α', Tools.resolveExpression("'\u03B1'"));
    }

    @Test
    public void testIntegerLitteral() {
        Assert.assertEquals(1, Tools.resolveExpression("1"));
        Assert.assertEquals(100, Tools.resolveExpression("0100"));
        Assert.assertEquals(Long.MAX_VALUE, Tools.resolveExpression("" + Long.MAX_VALUE));
        Assert.assertEquals(new BigInteger("19223372036854775807"), Tools.resolveExpression("1" + Long.MAX_VALUE));
        Assert.assertEquals(10, Tools.resolveExpression("0xa"));
        Assert.assertEquals(511, Tools.resolveExpression("0o777"));
        Assert.assertEquals(7, Tools.resolveExpression("0b111"));
    }

    @Test
    public void testNullLitteral() {
        Assert.assertEquals(NullOrMissingValue.NULL, Tools.resolveExpression("null"));
    }

    @Test
    public void formatter1() throws ProcessorException {
        String expressionScript = "[a] == \"${a}\"";
        Event ev = factory.newEvent();
        ev.put("a", "a");
        Boolean b = (Boolean) Tools.evalExpression(expressionScript, ev);
        Assert.assertTrue(b);
    }

    @Test
    public void formatter2() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        Object o = Tools.evalExpression("[a b] + \"${a.b}\"", ev);
        Assert.assertEquals("11", o);
    }

    @Test
    public void testMultiFormat() throws ProcessorException {
        String format = "\"${a%s} ${b%02d}\"";
        Expression expression = Tools.parseExpression(format);
        Event ev = factory.newEvent();
        ev.put("a", "1");
        ev.put("b", 2);
        Object o = expression.eval(ev);
        Assert.assertEquals("1 02", o);
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
        Assert.assertEquals("[a] - [b]", -1.1, results.get("[a] - [b]"), 1e-3);
        Assert.assertEquals("[b] - [c]", 1.1, results.get("[b] - [c]"), 1e-3);
        Assert.assertEquals("[c] - [d]", -1.1, results.get("[c] - [d]"), 1e-3);
        Assert.assertEquals("[d] - [a]", 1.1, results.get("[d] - [a]"), 1e-3);
        Expression exp = Tools.parseExpression("[c] - 1");
        Assert.assertThrows(IgnoredEventException.class, () -> exp.eval(ev));
    }

    @Test
    public void testCollectionList() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        ev.put("b", List.of(1, 2, 3));
        ev.put("c", Set.of(1, 2, 3));
        ev.put("d", new Integer[]{1, 2, 3});
        Assert.assertEquals(List.of('a', "a", 1), Tools.evalExpression("list('a', \"a\", [a])", ev));
        Assert.assertEquals(List.of(), Tools.evalExpression("list()", ev));
    }

    @Test
    public void testCollectionSet() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals(Set.of('a', "a", 1), Tools.evalExpression("set('a', 'a', \"a\", \"a\", [a])", ev));
        Assert.assertEquals(Set.of(), Tools.evalExpression("set()", ev));
    }

    @Test
    public void testContext() throws ProcessorException, UnknownHostException {
        Event ev = factory.newEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 31712), new InetSocketAddress("www.google.com", 443), null));
        Principal p = () -> "user";
        ev.getConnectionContext().setPrincipal(p);
        Assert.assertEquals(InetAddress.getByName("127.0.0.1"), Tools.evalExpression("[@context localAddress address]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[@context localAddress address] == \"127.0.0.1\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[@context localAddress port] == 31712", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[@context remoteAddress address] == \"www.google.com\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[@context remoteAddress port] == 443", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[@context principal name] == \"user\"", ev));

        // Checking both order of expression, change the way groovy handle it
        Assert.assertEquals(true, Tools.evalExpression("\"user\" == [@context principal name]", ev));
    }

    @Test
    public void testContextNullOrMissing() throws ProcessorException {
        Event ev = factory.newEvent();
        Principal p = () -> null;
        ev.getConnectionContext().setPrincipal(p);
        Assert.assertTrue((boolean) Tools.evalExpression("isEmpty([@context principal name])", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("isEmpty([@context localAddress])", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[@context localAddress port] == 443", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[@context bad] == 443", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[@context remoteAddress path] == 443", ev));
    }

    @Test
    public void testDottedContext() throws ProcessorException {
        Event ev = factory.newEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80), null));
        Principal p = () -> "user";
        ev.getConnectionContext().setPrincipal(p);
        Object value = Tools.evalExpression("[@context.principal.name] == \"user\"", ev);
        Assert.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) Tools.evalExpression("[ @context.localAddress]", ev);
        Assert.assertEquals(35710, localAddr.getPort());
    }

    private Object resolve(String function, String value) throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", value);
        return Tools.evalExpression(String.format("%s([a])", function),ev);
    }

    @Test
    public void testSplit() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "1 2 3");
        ev.put("b", "1/2/3");
        ev.put("c", null);
        Assert.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(\" \", [a])",ev));
        Assert.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(/ /, [a])",ev));
        Assert.assertEquals(List.of("1", "2", "3"), Tools.evalExpression("split(\"\"\"/\"\"\", [b])",ev));
        Assert.assertNull(Tools.evalExpression("split(\" \", [c])", ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("split(/ /, [d])",ev));
    }

    @Test
    public void testGsub() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "1 2 3");
        ev.put("b", "1/2/3");
        ev.put("c", null);
        Assert.assertEquals("1-2-3", Tools.evalExpression("gsub([a], / /, \"-\")",ev));
        Assert.assertEquals("1/2/3", Tools.evalExpression("gsub([b], / /, \"-\")",ev));
        Assert.assertEquals("1-2-3", Tools.evalExpression("gsub([b], \"\"\"/\"\"\", \"-\")",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("gsub([a], \"\"\"/\"\"\", \"-\")",ev));
        Assert.assertNull(Tools.evalExpression("gsub([c], / /, \"-\")", ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("gsub([d], / /, \"-\")",ev));
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
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", set(1, 2, 3))",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", list(1, 2, 3))",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [a])",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [aL])",ev));
        Assert.assertEquals("1.0 2.0 3.0", Tools.evalExpression("join(\" \", [af])",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [ac])",ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", [b])",ev));
        Assert.assertNull(Tools.evalExpression("join(\" \", [d])", ev));
        Assert.assertEquals("1 2 3", Tools.evalExpression("join(\" \", \"1 2 3\")",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("join(\" \", [e])",ev));
    }

    @Test
    public void testStringOperator() throws ProcessorException {
        Assert.assertEquals("abc", resolve("trim", " abc "));
        Assert.assertNull(resolve("trim", null));
        Assert.assertEquals("Abc", resolve("capitalize", "abc"));
        Assert.assertNull(resolve("capitalize", null));
        Assert.assertEquals("abc", resolve("uncapitalize", "Abc"));
        Assert.assertNull(resolve("uncapitalize", null));
        Assert.assertEquals("abc", resolve("lowercase", "AbC"));
        Assert.assertNull(resolve("lowercase", null));
        Assert.assertEquals("ABC", resolve("uppercase", "abC"));
        Assert.assertNull(resolve("uppercase", null));
        Assert.assertEquals(false, resolve("isBlank", "abc"));
        Assert.assertEquals(true, resolve("isBlank", ""));
        Assert.assertEquals(true, resolve("isBlank", " "));
        Assert.assertEquals(true, resolve("isBlank", null));
        Assert.assertEquals("a\nb\nc\nd", resolve("normalize", "a\nb\r\nc\rd"));
        Assert.assertNull(resolve("normalize", null));
        Event ev = factory.newEvent();
        ev.put("a", "1");
        ev.put("b", "2");
        Assert.assertEquals("12", Tools.evalExpression("[a] + [b]",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isBlank([c])",ev) );
    }

    @Test
    public void testCharOperator() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", ' ');
        ev.put("b", '1');
        ev.put("c", '2');
        Assert.assertEquals("", Tools.evalExpression("trim([a])",ev));
        Assert.assertEquals("12", Tools.evalExpression("[b] + [c]",ev));
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
        Assert.assertEquals(toEval, Tools.resolveExpression(expression));
    }

    @Test
    public void testNow() throws ProcessorException {
        Instant now = (Instant) Tools.evalExpression("now");
        Assert.assertTrue(Math.abs(Instant.now().getEpochSecond() - now.getEpochSecond()) < 1);
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
        for (Object o: new Object[] {
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
            Assert.assertTrue((boolean) Tools.evalExpression("isEmpty([a])", ev));
            Assert.assertFalse((boolean) Tools.evalExpression("! isEmpty([a])", ev));
        }
        Event ev = factory.newEvent();
        Assert.assertTrue((boolean) Tools.evalExpression("isEmpty([.])", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("! isEmpty([.])", ev));
    }


    @Test
    public void testIsEmptyIgnore() {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("event", "type"), "debug");
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isEmpty([event start])", ev));
        Assert.assertThrows(IgnoredEventException.class, () -> Tools.evalExpression("isEmpty([a])", ev));
    }

    @Test
    public void testIsEmptyFalse() throws ProcessorException {
        for (Object o: new Object[] {
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
            Assert.assertFalse((boolean) Tools.evalExpression("isEmpty([a])", ev));
            Assert.assertTrue((boolean) Tools.evalExpression("! isEmpty([a])", ev));
        }
    }

    @Test
    public void testExistsTrue() throws ProcessorException {
        Event ev = factory.newEvent(new IpConnectionContext(InetSocketAddress.createUnresolved("localhost", 0), InetSocketAddress.createUnresolved("localhost", 0), null));
        ev.putAtPath(VariablePath.of("event", "type"), "debug");
        ev.putAtPath(VariablePath.of("empty"), null);
        Assert.assertTrue((Boolean) Tools.evalExpression("[event type] == *", ev));
        Assert.assertTrue((Boolean) Tools.evalExpression("[empty] == *", ev));
        Assert.assertTrue((Boolean) Tools.evalExpression("[@context localAddress port] == *", ev));
    }

    @Test
    public void testExistsFalse() throws ProcessorException {
        Event ev = factory.newEvent();
        Assert.assertFalse((Boolean) Tools.evalExpression("[a] == *", ev));
        Assert.assertFalse((Boolean) Tools.evalExpression("[@context localAddress port] == *", ev));
    }

    @Test
    public void optimizeVarFormat() {
        String lambda = "\">${#1%s}<\"(1)";
        Assert.assertEquals(">1<", ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void optimizeVarFormatWithEvent() {
        String lambda = "\"${%j}\"";
        Assert.assertEquals(Expression.class, ConfigurationTools.unWrap(lambda, RouteParser::expression).getClass());
    }

    @Test
    public void optimizeConstantNumeric() {
        String lambda = "1";
        Assert.assertEquals(Integer.valueOf(1), ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void optimizeConstantString() {
        String lambda = "\"1\"";
        Assert.assertEquals("1", ConfigurationTools.unWrap(lambda, RouteParser::expression));
    }

    @Test
    public void parseLambda() throws ProcessorException {
        String lambda = "x -> x + 1";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assert.assertEquals(2, l.getExpression().eval(null, 1));
    }

    @Test
    public void parseLambdaConstant() throws ProcessorException {
        String lambda = "x -> 1";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assert.assertEquals(1, l.getExpression().eval(null, 1));
    }

    @Test
    public void parseLambdaFormatter() throws ProcessorException {
        String lambda = "x -> \">${#1%s}<\"(x)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assert.assertEquals(">1<", l.getExpression().eval(null, 1));
    }

    @Test
    public void parseLambdaConstantFormatter() throws ProcessorException {
        String lambda = "x -> \">${#1%s}<\"(1)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assert.assertEquals(">1<", l.getExpression().eval(null, null));
    }

    @Test
    public void parseLambdaIsEmpty() throws ProcessorException {
        String lambda = "x -> isEmpty(x)";
        Lambda l = ConfigurationTools.unWrap(lambda, RouteParser::lambda);
        Assert.assertEquals(true, l.getExpression().eval(null, List.of()));
    }

    @Test
    public void parseBadLambda() {
        String lambda = "x -> y + 1";
        ConfigException ex = Assert.assertThrows(ConfigException.class, () -> ConfigurationTools.unWrap(lambda, RouteParser::lambda));
        Assert.assertEquals("Invalid lambda definition", ex.getMessage());
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

        Assert.assertTrue((boolean) Tools.evalExpression("[a] == \"192.168.1.1\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[a] == \"/192.168.1.1\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[b] == \"www.google.com\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[c] == \"localhost\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[e] == \"127.0.0.1\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[e] == \"::1\"", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"192.168.1.1\" == [a]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"/192.168.1.1\" == [a]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"www.google.com\" == [b]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"localhost\" == [c]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"127.0.0.1\" == [e]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("\"::1\" == [e]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("isIP([a])", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("isIP(\"192.168.1.1\")", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("isIP(\"www.google.com\")", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("isIP(\"::1\")", ev));
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
        Assert.assertTrue((boolean) Tools.evalExpression("[a] && [b]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[a] && [c]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[b] && [c]", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[a] && [d]", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[a] && [e]", ev));
        Assert.assertFalse((boolean) Tools.evalExpression("[b] && [f]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[a] || [d]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[a] || [e]", ev));
        Assert.assertTrue((boolean) Tools.evalExpression("[b] || [f]", ev));
    }

    @Test
    public void constantOptimization() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(1);
        ExpressionBuilder b2 = ExpressionBuilder.of(2);
        ExpressionBuilder b3 = ExpressionBuilder.of(b1, "+", b2);
        Assert.assertEquals(3, b3.build("1+2").eval());
    }

    @Test
    public void staticFormat() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(new VarFormatter("1"));
        Assert.assertEquals("1", b1.build("1").eval());
    }

    @Test
    public void lambdaOptimization() throws ProcessorException {
        ExpressionBuilder b1 = ExpressionBuilder.of(1);
        ExpressionBuilder b2 = ExpressionBuilder.of(b1, o -> Expression.instanceOf(false, o, Integer.class));
        ExpressionBuilder b3 = ExpressionBuilder.of(b1, o -> Expression.instanceOf(true, o, Integer.class));
        Assert.assertEquals(true, b2.build("1 instanceof Integer").eval());
        Assert.assertEquals(false, b3.build("1 ! instanceof Integer").eval());
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
        Assert.assertEquals(1, map.get("b"));
        Assert.assertEquals(NullOrMissingValue.NULL, map.get("c"));
        Assert.assertEquals(List.of(NullOrMissingValue.NULL, NullOrMissingValue.NULL), map.get("d"));
    }

}
