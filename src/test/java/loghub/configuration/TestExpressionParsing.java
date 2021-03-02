package loghub.configuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

    private String parseExpression(String exp, Map<String, VarFormatter> formats) {
        return ConfigurationTools.unWrap(exp, i -> i.expression(), formats);
    }

    private Object evalExpression(String exp, Event ev, Map<String, VarFormatter> formats) throws ExpressionException, ProcessorException {
        Expression expression = new Expression(parseExpression(exp, formats), new Properties(Collections.emptyMap()).groovyClassLoader, formats);
        return expression.eval(ev);
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
    public void testTimestamp() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.setTimestamp(new Date(0));
        Date ts = (Date) evalExpression("[ @timestamp ]",ev);
        Assert.assertEquals(0L, ts.getTime());
    }

    @Test
    public void testMeta() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.putMeta("a", 1);
        Number i = (Number) evalExpression("[ #a ]",ev);
        Assert.assertEquals(1, i.intValue());
    }

    @Test
    public void testArray() throws ExpressionException, ProcessorException {
        Event ev =  Tools.getEvent();
        ev.put("a", new Integer[] { 1, 2, 3});
        Number i = (Number) evalExpression("[a][0]",ev);
        Assert.assertEquals(1, i.intValue());
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
        Object value = evalExpression("[ @context principal name ] == \"user\"", ev);
        Assert.assertEquals(true, value);
        InetSocketAddress localAddr = (InetSocketAddress) evalExpression("[ @context localAddress]", ev, Collections.singletonMap("h_" + formatHash, new VarFormatter(format)));
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
    public void testNow() throws ExpressionException, ProcessorException {
        Instant now = (Instant) evalExpression("now");
        Assert.assertTrue(Math.abs(Instant.now().getEpochSecond() - now.getEpochSecond()) < 1);
    }

}
