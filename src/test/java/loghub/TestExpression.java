package loghub;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
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
        VariablePath vp = VariablePath.of("value");
        VarFormatter format = new VarFormatter("${value}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = vp.groovyExpression() + " == formatters.faaf.format(event)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = factory.newEvent();
        ev.put("value", "a");
        Boolean b = (Boolean) expression.eval(ev);
        Assert.assertTrue(b);
    }

    @Test
    public void test2() throws ExpressionException, ProcessorException {
        VariablePath vp = VariablePath.of("a", "b");
        VarFormatter format = new VarFormatter("${b}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = vp.groovyExpression() + " + formatters.faaf.format(event.a)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = factory.newEvent();
        ev.put("a", Map.of("b", 1));
        Object o = expression.eval(ev);
        Assert.assertEquals("11", o);
    }

    @Test
    public void test3() throws ExpressionException, ProcessorException {
        String expressionScript = "\"a\"";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
        Event ev = factory.newEvent();
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", "a", o);
    }

  @Test
    public void testStringFormat() throws ProcessorException {
        String format = "${a%s} ${b%02d}";
        Expression expression = new Expression(format);
        Event ev = factory.newEvent();
        ev.put("a", "1");
        ev.put("b", 2);
        Object o = expression.eval(ev);
        Assert.assertEquals("1 02", o);
    }

    @Test
    public void testFailsCompilation() {
        // An expression valid in loghub, but not in groovy, should be catched
        String expressionScript = "event.getGroovyPath(\"host\") instanceof formatters.h_473e3665.format(event)";
        ExpressionException ex = Assert.assertThrows(ExpressionException.class, 
                () -> new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap()));
        Assert.assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
    }

    @Test
    public void testDateDiff() {
        Instant now = Instant.now();
        Event ev = factory.newEvent();
        ev.put("a", now.minusMillis(1100));
        ev.put("b", now);
        ev.put("c", Date.from(now.minusMillis(1100)));
        ev.put("d", Date.from(now));
        String[] scripts = new String[] { "event.a - event.b", "event.b - event.c", "event.c - event.d", "event.d - event.a"};
        Map<String, Double> results = new HashMap<>(scripts.length);
        Arrays.stream(scripts).forEach(s -> {
            try {
                Expression exp = new Expression(s, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
                double f = (double) exp.eval(ev);
                results.put(s, f);
            } catch (ExpressionException | ProcessorException e) {
                throw new RuntimeException(e);
            }
        });
        Assert.assertEquals("event.a - event.b", -1.1, results.get("event.a - event.b"), 1e-3);
        Assert.assertEquals("event.b - event.c", 1.1, results.get("event.b - event.c"), 1e-3);
        Assert.assertEquals("event.c - event.d", -1.1, results.get("event.c - event.d"), 1e-3);
        Assert.assertEquals("event.d - event.a", 1.1, results.get("event.d - event.a"), 1e-3);
        Assert.assertThrows(IgnoredEventException.class, () -> {
            Expression exp = new Expression("event.c -1", new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
            exp.eval(ev);
            Assert.fail();
        });
    }

    @Test
    public void testNull() throws ProcessorException, ExpressionException {
        Event ev = factory.newEvent();
        ev.put("a", null);
        Assert.assertNull(new Expression(NullOrMissingValue.NULL).eval(null));
        Assert.assertNull(new Expression(VariablePath.of("a")).eval(ev));
        Expression nullexp = new Expression("null", new Properties(Collections.emptyMap()).groovyClassLoader,
                Collections.emptyMap());
        Assert.assertNull(nullexp.eval(ev));
    }

    @Test
    public void testWithArg() throws ProcessorException, ExpressionException {
        String expressionScript = "value + 1";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
        Event ev = factory.newEvent();
        Object o = expression.eval(ev, 2);
        Assert.assertEquals(3, o);
    }

}
