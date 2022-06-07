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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.IgnoredEventException;
import loghub.IpConnectionContext;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.VarFormatter;

public class TestExpressionParsing {

    private static Logger logger ;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.configuration", "loghub.Expression", "loghub.VarFormatter");
    }

    private Expression parseExpression(String exp, Map<String, VarFormatter> formats) {
        return ConfigurationTools.unWrap(exp, i -> i.expression(), formats);
    }

    private Object evalExpression(String exp, Event ev, Map<String, VarFormatter> formats) throws ExpressionException, ProcessorException {
        return parseExpression(exp, formats).eval(ev);
    }

    private Object evalExpression(String exp, Event ev) throws ExpressionException, ProcessorException {
        return evalExpression(exp, ev, new HashMap<>());
    }

    private Object evalExpression(String exp) throws ExpressionException, ProcessorException {
        return evalExpression(exp, Tools.getEvent());
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
        Event ev =  Tools.getEvent();
        ev.put("a", 1);
        Assert.assertEquals("01", evalExpression("\"" + format + "\"([a])", ev));
    }

    @Test(expected=ProcessorException.class)
    public void testFormatterFailed() throws ExpressionException, ProcessorException {
        String format = "${#2%02d}";
        Event ev =  Tools.getEvent();
        ev.put("a", 1);
        try {
            evalExpression("\"" + format + "\"([a])", ev);
        } catch (ProcessorException e) {
            Assert.assertTrue(e.getMessage().contains("index out of range"));
            throw e;
        }
    }

    @Test
    public void testFormatterTimestamp() throws ExpressionException, ProcessorException {
        String format = "${#1%t<Europe/Paris>H}";
        Event ev =  Tools.getEvent();
        ev.setTimestamp(new Date(0));
        Assert.assertEquals("01", evalExpression("\"" + format + "\"([@timestamp])", ev));
    }

    @Test
    public void testFormatterContextPrincipal() throws ExpressionException, ProcessorException {
        String format = "${#1%s}-${#2%tY}.${#2%tm}.${#2%td}";
        Event ev =  Event.emptyEvent(new ConnectionContext<Object>() {
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
        Principal p = new Principal() {
            @Override
            public String getName() {
                return "user";
            }
        };
        ev.getConnectionContext().setPrincipal(p);
        Assert.assertEquals("user-1970.01.01", evalExpression("\"" + format + "\" ([@context principal name], [@timestamp])", ev));
    }

    @Test
    public void testFormatterEvent() throws ExpressionException, ProcessorException {
        String format = "${a}";
        Event ev =  Tools.getEvent();
        ev.put("a", 1);
        Assert.assertEquals("1", evalExpression("\"" + format + "\"", ev));
    }

    @Test
    public void testEventPath() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", "c"));
        Assert.assertEquals("c", evalExpression("[a b]", ev));
    }

    @Test
    public void testEventPathQuoted() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", "c"));
        Assert.assertEquals("c", evalExpression("[\"a\" \"b\"]", ev));
    }

    @Test
    public void testEventPathDotted() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", "c"));
        Assert.assertEquals("c", evalExpression("[a.b]", ev));
    }

    @Test
    public void testContextPattern() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        String result = (String) evalExpression("\"${#1%s} ${#2%s}\"([@context], [@context principal]) ", ev);
        Assert.assertTrue(Pattern.matches("loghub.ConnectionContext\\$1@[a-f0-9]+ loghub.ConnectionContext\\$EmptyPrincipal@[a-f0-9]+", result));
    }

    private void enumerateExpressions(Event ev, Object[] tryExpression) {
        Map<String, Object> tests = new HashMap<>(tryExpression.length / 2);
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
    public void testOperators() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        Object[] tryExpression = new Object[] {
                "1 instanceof java.lang.Integer", true,
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
    public void testValueMissing() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        Object[] tryExpression = new Object[] {
                "null == [a b]", true,
                "[a b] == null", true,
                "[a b] != null", false,
                "[a b] === null", false,
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
    public void testNullValue() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", null);
        Object[] tryExpression = new Object[] {
                "null == [a]", true,
                "[a] == null", true,
                "[a] != null", false,
                "[a] === null", true,
                "2 == [a]", false,
                "[a] == 2", false,
                "[a] instanceof java.lang.Integer", false,
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
                ".~ [a]", IgnoredEventException.class,
                "! [a]", true,
                "+ [a]", IgnoredEventException.class,
                "- [a]", IgnoredEventException.class,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testComparaison() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        Object[] tryExpression = new Object[] {
                "2 <=> 3", Integer.compare(2, 3),
                "3 <=> 2", Integer.compare(3, 2),
                "2 <=> 2", Integer.compare(2, 2),
                "2 < 3", 2 < 3,
                "3 > 2", 3 > 2,
                "2 <= 3", 2 <= 3,
                "3 <= 2", 3 <= 2,
                "2 <= 2", 2 <= 2,
                "2 >= 2", 2 >= 2,
        };
        enumerateExpressions(ev, tryExpression);
    }

    @Test
    public void testTimestamp() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.setTimestamp(new Date(0));
        Date ts = (Date) evalExpression("[ @timestamp ]", ev);
        Assert.assertEquals(0L, ts.getTime());
    }

    @Test
    public void testNotTimestamp() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.setTimestamp(new Date(0));
        ev.put(Event.TIMESTAMPKEY, 1);

        Object v = evalExpression("[ \"@timestamp\" ]", ev);
        Assert.assertEquals(1, v);
    }

    @Test
    public void testMeta() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.putMeta("a", 1);
        Number i = (Number) evalExpression("[ #a ]", ev);
        Assert.assertEquals(1, i.intValue());
    }

    @Test
    public void testArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        Number i = (Number) evalExpression("[a][2]", ev);
        Assert.assertEquals(3, i.intValue());
    }

    @Test(expected=IgnoredEventException.class)
    public void testArrayOutOfBound() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        evalExpression("[a][3]", ev);
    }

    @Test
    public void testList() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", Stream.of(1, 2, 3).collect(Collectors.toList()));
        Number i = (Number) evalExpression("[a][2]", ev);
        Assert.assertEquals(3, i.intValue());
    }

    @Test(expected=IgnoredEventException.class)
    public void testListOutOfBound() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", Stream.of(1, 2, 3).collect(Collectors.toList()));
        evalExpression("[a][3]", ev);
    }

    @Test(expected=IgnoredEventException.class)
    public void testMissingArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        evalExpression("[b][3]", ev);
    }

    @Test(expected=ProcessorException.class)
    public void testNotArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", 1);
        evalExpression("[a][0]", ev);
    }

    @Test
    public void testPatternBoolean() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", "abc");
        Boolean i = (Boolean) evalExpression("[a] ==~ /(a.)(.)/",ev);
        Assert.assertEquals(true, i.booleanValue());
    }

    @Test
    public void testPatternBooleanEscaped() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", "a.c\n");
        Boolean i = (Boolean) evalExpression("[a] ==~ /a\\.c\\n/",ev);
        Assert.assertEquals(true, i.booleanValue());
    }

    @Test
    public void testPatternArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", "abc");
        String i = (String) evalExpression("([a] =~ /(a.)(.)/)[2]",ev);
        Assert.assertEquals("c", i);
    }

    @Test
    public void testFailedPatternArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", "abc");
        Object found = evalExpression("([a] =~ /d.*/)[2]",ev);
        Assert.assertEquals(null, found);
    }

    @Test
    public void testStringLitteral() throws ExpressionException, ProcessorException {
        String format = "b";
        Event ev =  Tools.getEvent();
        ev.put("a", 1);
        Assert.assertEquals("b", evalExpression("\"" + format + "\"", ev));
    }

    @Test
    public void testContext() throws ExpressionException, ProcessorException {
        String format = "user";
        String formatHash = Integer.toHexString(format.hashCode());

        Event ev =  Event.emptyEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80), null));
        Principal p = new Principal() {
            @Override
            public String getName() {
                return "user";
            }
        };
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

        Event ev =  Event.emptyEvent(new IpConnectionContext(new InetSocketAddress("127.0.0.1", 35710), new InetSocketAddress("localhost", 80), null));
        Principal p = new Principal() {
            @Override
            public String getName() {
                return "user";
            }
        };
        ev.getConnectionContext().setPrincipal(p);
        Object value = evalExpression("[ @context.principal.name ] == \"user\"", ev);
        Assert.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) evalExpression("[ @context.localAddress]", ev, Collections.singletonMap("h_" + formatHash, new VarFormatter(format)));
        Assert.assertEquals(35710, localAddr.getPort());
    }

    private Object resolve(String function, String value) throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", value);
        return evalExpression(String.format("%s([a])", function),ev);
    }

    @Test
    public void testStringOperator() throws ExpressionException, ProcessorException {
        Assert.assertEquals("abc", resolve("trim", " abc "));
        Assert.assertEquals("Abc", resolve("capitalize", "abc"));
        Assert.assertEquals("abc", resolve("uncapitalize", "Abc"));
        Assert.assertEquals(false, resolve("isBlank", "abc"));
        Assert.assertEquals(true, resolve("isBlank", ""));
        Assert.assertEquals(true, resolve("isBlank", " "));
        Assert.assertEquals(true, resolve("isBlank", null));
        Assert.assertEquals("a\nb\nc\nd", resolve("normalize", "a\nb\r\nc\rd"));
    }

    @Test
    public void testComplexString() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        String toEval = "a\"\\\t\n\r" + String.valueOf(Character.toChars(0x10000));
        StringBuffer buffer = new StringBuffer();
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
              .forEach(s -> {
                  parseExpression(s, Collections.emptyMap());
               });
        }
    }

}
