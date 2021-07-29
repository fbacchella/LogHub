package loghub;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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

public class TestExpression {

    private static Logger logger ;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.Expression", "loghub.VarFormatter");
        Expression.clearCache();
    }

    @Test
    public void test1() throws ExpressionException, ProcessorException {
        VarFormatter format = new VarFormatter("${value}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.getGroovyPath(\"value\") == formatters.faaf.format(event)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("value", "a");
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", true, (Boolean)o);
    }

    @Test
    public void test2() throws ExpressionException, ProcessorException {
        VarFormatter format = new VarFormatter("${b}");
        Map<String, VarFormatter> formatters = Collections.singletonMap("faaf", format);
        String expressionScript = "event.getGroovyPath(\"a\", \"b\") + formatters.faaf.format(event.a)";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, formatters);
        Event ev = Tools.getEvent();
        ev.put("a", Collections.singletonMap("b", 1));
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", "11", (String)o);
    }

    @Test
    public void test3() throws ExpressionException, ProcessorException {
        String expressionScript = "\"a\"";
        Expression expression = new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
        Event ev = Tools.getEvent();
        Object o = expression.eval(ev);
        Assert.assertEquals("failed to parse expression", "a", (String)o);
    }

    @Test
    public void testFailsCompilation() throws ExpressionException, ProcessorException {
        // An expression valid in loghub, but not in groovy, should be catched
        String expressionScript = "event.getGroovyPath(\"host\") instanceof formatters.h_473e3665.format(event)";
        ExpressionException ex = Assert.assertThrows(ExpressionException.class, 
                () -> new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap()));
        Assert.assertEquals(MultipleCompilationErrorsException.class, ex.getCause().getClass());
    }

    @Test
    public void testDateDiff() throws ExpressionException, ProcessorException {
        Instant now = Instant.now();
        Event ev = Tools.getEvent();
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
    public void dateCompare() {
        Instant now = Instant.now();
        Event ev = Tools.getEvent();
        ev.put("a", now);
        ev.put("b", Date.from(now));
        ev.put("c", ZonedDateTime.ofInstant(now, ZoneId.systemDefault()));
        ev.put("d", 1);
        String[] scripts = new String[] { "ex.compare(\"<=>\", event.a, event.b)", "ex.compare(\"<=>\", event.b, event.c)", "ex.compare(\"<=>\", event.c, event.a)"};
        Map<String, Integer> results = new HashMap<>(scripts.length);
        Arrays.stream(scripts).forEach(s -> {
            try {
                Expression exp = new Expression(s, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
                Integer f = (Integer) exp.eval(ev);
                results.put(s, f);
            } catch (ExpressionException | ProcessorException e) {
                throw new RuntimeException(e);
            }
        });
        Assert.assertEquals(scripts.length, results.size());
        results.forEach((k,v) -> {
            Assert.assertEquals(k, 0, v.intValue());
        });
        Assert.assertThrows(IgnoredEventException.class, () -> {
            Expression exp = new Expression("ex.compare(\"<=>\", event.a, event.d)", new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
            exp.eval(ev);
            Assert.fail();
        });
    }

}
