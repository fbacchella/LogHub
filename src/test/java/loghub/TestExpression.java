package loghub;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestExpression {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression", "loghub.VarFormatter");
        Expression.clearCache();
    }

    @Test
    public void test1() throws ExpressionException, ProcessorException {
        String expressionScript = "[value] == \"${value}\"";
        Event ev = factory.newEvent();
        ev.put("value", "a");
        Boolean b = (Boolean) Tools.evalExpression(expressionScript, ev);
        Assert.assertTrue(b);
    }

    @Test
    public void test2() throws ExpressionException, ProcessorException {
        Event ev = factory.newEvent();
        ev.putAtPath(VariablePath.of("a", "b"), 1);
        Object o = Tools.evalExpression("[a b] + \"${a.b}\"", ev);
        Assert.assertEquals("11", o);
    }

    @Test
    public void test3() throws ProcessorException {
        Expression expression = Tools.parseExpression("\"a\"", Collections.emptyMap());
        Event ev = factory.newEvent();
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", "a", o);
    }

  @Test
    public void testStringFormat() throws ProcessorException {
        String format = "\"${a%s} ${b%02d}\"";
        Expression expression = Tools.parseExpression(format, new HashMap<>());
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
                Expression exp = Tools.parseExpression(s, Collections.emptyMap());
                //Expression exp = new Expression(s, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
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
        Assert.assertThrows(IgnoredEventException.class, () -> {
            Expression exp = Tools.parseExpression("[c] - 1", Collections.emptyMap());
            exp.eval(ev);
            Assert.fail();
        });
    }

    @Test
    public void testNull() throws ProcessorException {
        Event ev = factory.newEvent();
        ev.put("a", null);
        Assert.assertNull(new Expression(NullOrMissingValue.NULL).eval(null));
        Assert.assertNull(new Expression(VariablePath.of("a")).eval(ev));
        Expression nullexp = new Expression("null", new Properties(Collections.emptyMap()).groovyClassLoader,
                Collections.emptyMap());
        Assert.assertNull(nullexp.eval(ev));
    }

    @Test
    public void testWithArg() throws ProcessorException {
        String expressionScript = "value + 1";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
        Event ev = factory.newEvent();
        Object o = expression.eval(ev, 2);
        Assert.assertEquals(3, o);
    }

}
