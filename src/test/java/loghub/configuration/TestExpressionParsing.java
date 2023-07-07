package loghub.configuration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.antlr.v4.runtime.RecognitionException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.IgnoredEventException;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.RouteParser;
import loghub.Tools;
import loghub.VarFormatter;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestExpressionParsing {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.Expression", "loghub.VarFormatter");
    }

    private Expression parseExpression(String exp, Map<String, VarFormatter> formats) {
        return ConfigurationTools.unWrap(exp, RouteParser::expression, formats);
    }

    private Object evalExpression(String exp, Event ev, Map<String, VarFormatter> formats) throws ProcessorException {
        return parseExpression(exp, formats).eval(ev);
    }

    private Object evalExpression(String exp, Event ev) throws ExpressionException, ProcessorException {
        return evalExpression(exp, ev, new HashMap<>());
    }

    private Object evalExpression(String exp) throws ExpressionException, ProcessorException {
        return evalExpression(exp, factory.newEvent());
    }

    @Test
    public void testSimple() throws ExpressionException, ProcessorException {
        Assert.assertEquals("3", evalExpression("1 + 2").toString());
    }

    @Test
    public void testOr() throws ExpressionException, ProcessorException {
        Assert.assertEquals("3", evalExpression("1 .| 2").toString());
    }

    @Test
    public void testUnary() throws ExpressionException, ProcessorException {
        Assert.assertEquals("2", evalExpression("-(-2)").toString());
        Assert.assertEquals("-2", evalExpression(".~1").toString());
        Assert.assertEquals("false", evalExpression("!1").toString());
    }

    @Test
    public void testSubExpression() throws ExpressionException, ProcessorException {
        Assert.assertEquals("12", evalExpression("(1 + 2) * 4").toString());
    }

    @Test
    public void testNew() throws ExpressionException, ProcessorException {
        Date newdate = (Date)evalExpression("new java.util.Date(1+2)");
        Assert.assertEquals(new Date(3), newdate);
    }

    @Test
    public void testFormatterSimple() throws ExpressionException, ProcessorException {
        String format = "${#1%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals("01", evalExpression("\"" + format + "\"([a])", ev));
    }

    @Test
    public void testFormatterFailed() {
        String format = "${#2%02d}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        ProcessorException pe = Assert.assertThrows(ProcessorException.class, () -> evalExpression("\"" + format + "\"([a])", ev));
        Assert.assertTrue(pe.getMessage().contains("index out of range"));
    }

    @Test
    public void testFormatterTimestamp() throws ExpressionException, ProcessorException {
        String format = "${#1%t<Europe/Paris>H}";
        Event ev =  factory.newEvent();
        ev.setTimestamp(new Date(0));
        Assert.assertEquals("01", evalExpression("\"" + format + "\"([@timestamp])", ev));
    }

    @Test
    public void testFormatterContextPrincipal() throws ExpressionException, ProcessorException {
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
        Assert.assertEquals("user-1970.01.01", evalExpression("\"" + format + "\" ([@context principal name], [@timestamp])", ev));
    }

    @Test
    public void testFormatterEvent() throws ExpressionException, ProcessorException {
        String format = "${a}";
        Event ev =  factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals("1", evalExpression("\"" + format + "\"", ev));
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
    public void testContextPattern() throws ExpressionException, ProcessorException {
        Event ev =  factory.newEvent();
        String result = (String) evalExpression("\"${#1%s} ${#2%s}\"([@context], [@context principal]) ", ev);
        Assert.assertTrue(Pattern.matches("loghub.ConnectionContext\\$1@[a-f0-9]+ loghub.ConnectionContext\\$EmptyPrincipal@[a-f0-9]+", result));
    }

    private void enumerateExpressions(Event ev, Object[] tryExpression) {
        Map<String, Object> tests = new LinkedHashMap<>(tryExpression.length / 2);
        for (int i = 0; i < tryExpression.length; ) {
            tests.put(tryExpression[i++].toString(), tryExpression[i++]);
        }
        tests.forEach((x, r) -> {
            try {
                Object o = evalExpression(x, ev);
                Assert.assertEquals(x, r, o);
            } catch (IgnoredEventException e) {
                if ( r != IgnoredEventException.class) {
                    Assert.fail(x);
                }
            } catch (ProcessorException e) {
                if ( r != ProcessorException.class) {
                    Assert.fail(x);
                }
            } catch (ExpressionException e) {
                Assert.fail(x);
            }
        });
    }

    @Test
    public void testOperators() {
        Event ev = factory.newEvent();
        Object[] tryExpression = new Object[] {
                "1 instanceof java.lang.Integer", true,
                "1 !instanceof java.lang.Integer", false,
                "'a' in \"bac\"", true,
                "'d' in \"bac\"", false,
                "1 in list(1,2,3)", true,
                "4 in list(1,2,3)", false,
                "2 ** 3", 8,
                "2 ** (999999999 +1)", Double.NaN,
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
                "2 > 2]", false,
                "2 == 2", true,
                "2 === 2", true,
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
                "null == [a b]", true,
                "[a b] == null", true,
                "[a b] != null", false,
                "[a b] === null", false,
                "2 == [a b]", false,
                "[a b] == 2", false,
                "[a b] instanceof java.lang.Integer", false,
                "2 instanceof [a b]", false,
                "null instanceof [a b]", false,
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
                "true && [a b]", false,
                "false || [a b]", false,
                "2 in [a b]", false,
                ".~ [a b]", IgnoredEventException.class,
                "! [a b]", true,
                "+ [a b]", IgnoredEventException.class,
                "- [a b]", IgnoredEventException.class,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testNullValue() {
        Event ev = factory.newEvent();
        ev.put("a", null);
        Object[] tryExpression = new Object[] {
                "null == [a]", true,
                "[a] == null", true,
                "[a] != null", false,
                "[a] === null", true,
                "2 == [a]", false,
                "[a] == 2", false,
                "[a] instanceof java.lang.Integer", false,
                "[a] !instanceof java.lang.Integer", true,
                "null instanceof [a]", true,
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
                "2 < 3", true,
                "2 < [a]", IgnoredEventException.class,
                "2 > [a]", IgnoredEventException.class,
                "2 <=> [a]", IgnoredEventException.class,
                "[a] <=> 2", IgnoredEventException.class,
                "2 .^ [a]", IgnoredEventException.class,
                "2 .| [a]", IgnoredEventException.class,
                "2 .& [a]", IgnoredEventException.class,
                "true && [a]", false,
                "false || [a]", false,
                "2 in [a]", false,
                "null in [a]", true,
                ".~ [a]", IgnoredEventException.class,
                "! [a]", true,
                "+ [a]", IgnoredEventException.class,
                "- [a]", IgnoredEventException.class,
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
    public void testTimestamp() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        Date ts = (Date) evalExpression("[@timestamp]", ev);
        Assert.assertEquals(0L, ts.getTime());
    }

    @Test
    public void testTimestampCompare() throws ProcessorException, ExpressionException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put("a", Instant.ofEpochMilli(0));
        Object o = evalExpression("[@timestamp] == [a]", ev);
        Assert.assertEquals(true, o);
    }

    @Test
    public void testNotTimestamp() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.setTimestamp(new Date(0));
        ev.put(Event.TIMESTAMPKEY, 1);

        Object v = evalExpression("[ \"@timestamp\" ]", ev);
        Assert.assertEquals(1, v);
    }

    @Test
    public void testMeta() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.putMeta("a", 1);
        Number i = (Number) evalExpression("[ #a ]", ev);
        Assert.assertEquals(1, i.intValue());
    }

    @Test
    public void testArrayJoin() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        ev.put("b", new Integer[] { 4, 5, 6});
        Object[] i = (Object[]) evalExpression("[a] + [b]", ev);
        Assert.assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6}, i);
    }

    @Test
    public void testNeedUnwrap() throws ProcessorException, ExpressionException {
        // This expression fails if unwrap of PojoWrapper is not done
        Set i = (Set) evalExpression("set(1, 2, 3) + 4", factory.newEvent());
        Assert.assertEquals(Set.of(1, 2, 3, 4), i);
    }

    @Test
    public void testArrayMixed() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        ev.put("b", Stream.of(4, 5, 6).collect(Collectors.toList()));
        ev.put("c", Stream.of(7, 8, 9).collect(Collectors.toSet()));
        Object[] i = (Object[]) evalExpression("[a] + [b] + [c]", ev);
        Assert.assertArrayEquals(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, i);
    }

    @Test
    public void testArray() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        Number i = (Number) evalExpression("[a][2]", ev);
        Assert.assertEquals(3, i.intValue());
    }

    @Test
    public void testArrayNegative() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] {1, 2, 3});
        ev.put("b", List.of(4, 5, 6));
        Number i1 = (Number) evalExpression("[a][-1]", ev);
        Assert.assertEquals(3, i1.intValue());
        Number i2 = (Number) evalExpression("[b][-1]", ev);
        Assert.assertEquals(6, i2.intValue());
    }

    @Test(expected=IgnoredEventException.class)
    public void testArrayOutOfBound() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        evalExpression("[a][3]", ev);
    }

    @Test
    public void testList() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", Stream.of(1, 2, 3).collect(Collectors.toList()));
        Number i = (Number) evalExpression("[a][2]", ev);
        Assert.assertEquals(3, i.intValue());
    }

    @Test(expected=IgnoredEventException.class)
    public void testListOutOfBound() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", Stream.of(1, 2, 3).collect(Collectors.toList()));
        evalExpression("[a][3]", ev);
    }

    @Test(expected=IgnoredEventException.class)
    public void testMissingArray() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        evalExpression("[b][3]", ev);
    }

    @Test(expected=ProcessorException.class)
    public void testNotArray() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        evalExpression("[a][0]", ev);
    }

    @Test
    public void testPatternBoolean() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        Boolean i = (Boolean) evalExpression("[a] ==~ /(a.)(.)/",ev);
        Assert.assertEquals(true, i);
    }

    @Test
    public void testPatternEmpty() throws ExpressionException, ProcessorException {
        Set<Object> toTest = Set.of(NullOrMissingValue.NULL,
                Collections.emptySet(), Collections.emptyMap(), Collections.emptyList(),
                new Object[]{});
        Event ev = factory.newEvent();
        for(Object o: toTest) {
            ev.put("a", o);
            Boolean i = (Boolean) evalExpression("[a] ==~ /.*/",ev);
            Assert.assertEquals(false, i);
        }
        ev.put("a", null);
        Boolean i = (Boolean) evalExpression("[a] ==~ /.*/",ev);
        Assert.assertEquals(false, i);
    }

    @Test(expected = IgnoredEventException.class)
    public void
    testPatternMissing() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", NullOrMissingValue.MISSING);
        evalExpression("[a] ==~ /.*/",ev);
    }

    @Test
    public void testPatternBooleanEscaped() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "a.c\n");
        Boolean i = (Boolean) evalExpression("[a] ==~ /a\\.c\\n/",ev);
        Assert.assertEquals(true, i);
    }

    @Test
    public void testPatternArray() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        String i = (String) evalExpression("([a] =~ /(a.)(.)/)[2]",ev);
        Assert.assertEquals("c", i);
    }

    @Test(expected = IgnoredEventException.class)
    public void testFailedPatternArray() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "abc");
        evalExpression("([a] =~ /d.*/)[2]",ev);
    }

    @Test(expected = RecognitionException.class)
    public void testBadPattern() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        evalExpression("[a] ==~ /*/",ev);
    }

    @Test
    public void testStringLitteral() throws ExpressionException, ProcessorException {
        String format = "b";
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals("b", evalExpression("\"" + format + "\"", ev));
    }

    @Test
    public void testCharacterLitteral() throws ExpressionException, ProcessorException {
        Assert.assertEquals('a', evalExpression("'a'"));
    }

    @Test
    public void testCollectionList() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals(List.of('a', "a", 1), evalExpression("list('a', \"a\", [a])", ev));
    }

    @Test
    public void testCollectionSet() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", 1);
        Assert.assertEquals(Set.of('a', "a", 1), evalExpression("set('a', 'a', \"a\", \"a\", [a])", ev));
    }

    @Test
    public void testContext() throws ExpressionException, ProcessorException {
        String format = "user";
        String formatHash = Integer.toHexString(format.hashCode());

        Event ev = factory.newEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80), null));
        Principal p = () -> "user";
        ev.getConnectionContext().setPrincipal(p);
        Object value = evalExpression("[@context principal name ] == \"user\"", ev);
        Assert.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) evalExpression("[@context localAddress]", ev, Collections.singletonMap("h_" + formatHash, new VarFormatter(format)));
        Assert.assertEquals(35710, localAddr.getPort());
    }

    @Test
    public void testDottedContext() throws ExpressionException, ProcessorException {
        String format = "user";
        String formatHash = Integer.toHexString(format.hashCode());

        Event ev = factory.newEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80), null));
        Principal p = () -> "user";
        ev.getConnectionContext().setPrincipal(p);
        Object value = evalExpression("[ @context.principal.name ] == \"user\"", ev);
        Assert.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) evalExpression("[ @context.localAddress]", ev, Collections.singletonMap("h_" + formatHash, new VarFormatter(format)));
        Assert.assertEquals(35710, localAddr.getPort());
    }

    private Object resolve(String function, String value) throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", value);
        return evalExpression(String.format("%s([a])", function),ev);
    }

    @Test
    public void testSplit() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", "1 2 3");
        ev.put("b", ' ');
        ev.put("c", null);
        Assert.assertArrayEquals(new String[]{"1", "2", "3"}, (String[])evalExpression("split(\" \", [a])",ev));
        Assert.assertArrayEquals(new String[]{"1", "2", "3"}, (String[])evalExpression("split([b], [a])",ev));
        Assert.assertEquals("1 2 3", evalExpression("split(null, [a])",ev));
        Assert.assertEquals(null, evalExpression("split(' ', [c]))",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> evalExpression("split(null, [d])",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> evalExpression("split([d], [a])",ev));
    }

    @Test
    public void testJoin() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", List.of(1, 2, 3));
        ev.put("aL", new Long[]{1L, 2L, 3L});
        ev.put("af", new float[]{1.0f, 2.0f, 3.0f});
        ev.put("ac", new char[]{'1', '2', '3'});
        ev.put("b", new TreeSet(List.of(1, 2, 3, 1)));
        ev.put("c", ' ');
        ev.put("d", null);
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", set(1, 2, 3))",ev));
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", list(1, 2, 3))",ev));
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", [a])",ev));
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", [aL])",ev));
        Assert.assertEquals("1.0 2.0 3.0", evalExpression("join(\" \", [af])",ev));
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", [ac])",ev));
        Assert.assertEquals("1 2 3", evalExpression("join(\" \", [b])",ev));
        Assert.assertEquals("1 2 3", evalExpression("join([c], [a])",ev));
        Assert.assertEquals("123", evalExpression("join(null, [a])",ev));
        Assert.assertEquals("123", evalExpression("join([d], [a])",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> evalExpression("split(' ', [e])",ev));
        Assert.assertThrows(IgnoredEventException.class, () -> evalExpression("split([e], [a])",ev));
    }

    @Test
    public void testStringOperator() throws ExpressionException, ProcessorException {
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
        Assert.assertEquals("12", evalExpression("[a] + [b]",ev));
    }

    @Test
    public void testComplexString() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
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
        Assert.assertEquals(toEval, evalExpression(expression, ev));
    }

    @Test
    public void testNow() throws ExpressionException, ProcessorException {
        Instant now = (Instant) evalExpression("now");
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
              .forEach(s -> parseExpression(s, Collections.emptyMap()));
        }
    }

    @Test
    public void testIsEmptyTrue() throws ProcessorException, ExpressionException {
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
            Assert.assertTrue((boolean) evalExpression("isEmpty([a])", ev));
        }
        Event ev = factory.newEvent();
        Assert.assertTrue((boolean) evalExpression("isEmpty([a])", ev));
    }

    @Test
    public void testIsEmptyFalse() throws ProcessorException, ExpressionException {
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
            Assert.assertFalse((boolean) evalExpression("isEmpty([a])", ev));
        }
    }

}
