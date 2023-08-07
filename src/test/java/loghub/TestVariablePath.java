package loghub;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestVariablePath {

    private static final Pattern  groovyPattern = Pattern.compile("event.getGroovyPath\\(\\d+\\)");

    @BeforeClass
    public static void reset() {
        VariablePath.reset();
    }

    @Test
    public void single() {
        VariablePath vp = VariablePath.parse("a");
        Assert.assertSame(vp, VariablePath.of("a"));
        Assert.assertEquals(vp, VariablePath.of(Collections.singletonList("a")));
        Assert.assertEquals("[a]", vp.toString());
        Assert.assertTrue(groovyPattern.matcher(vp.groovyExpression()).matches());
        Assert.assertSame(VariablePath.of("a", "b"), vp.append("b"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void pathological() throws ProcessorException {
        List<String> pathological = List.of("'", "\n", "\"", ".");
        VariablePath vp = VariablePath.of(pathological);
        Event ev = new EventsFactory().newEvent();
        ev.putAtPath(vp, true);
        Map<String, Object> sub = ev;
        for (String s: pathological) {
            if (! ".".equals(s)) {
                sub = (Map<String, Object>) sub.get(s);
            }
        }
        Assert.assertTrue((Boolean) sub.get("."));
        Expression expression = new Expression(vp);
        Assert.assertEquals(true, expression.eval(ev));
    }

    @Test
    public void empty() {
        Assert.assertSame(VariablePath.EMPTY, VariablePath.parse(""));
        Assert.assertSame(VariablePath.EMPTY, VariablePath.parse(" "));
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of());
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of(Collections.emptyList()));
        Assert.assertEquals("[]", VariablePath.EMPTY.toString());
        Assert.assertEquals("event", VariablePath.EMPTY.groovyExpression());
        Assert.assertSame(VariablePath.of("a"), VariablePath.EMPTY.append("a"));
    }

    private void checkABC(VariablePath vp) {
        Assert.assertFalse(vp.isTimestamp());
        Assert.assertFalse(vp.isMeta());
        Assert.assertFalse(vp.isIndirect());
        Assert.assertEquals(VariablePath.of("a", "b", "c"), vp);
        Assert.assertEquals(VariablePath.of(List.of("a","b", "c")), vp);
        Assert.assertEquals("[a.b.c]", vp.toString());
        Assert.assertTrue(groovyPattern.matcher(vp.groovyExpression()).matches());
    }

    @Test
    public void variantABC() {
        checkABC(VariablePath.parse("a.b.c"));
        checkABC(VariablePath.parse("a..b..c."));
        checkABC(VariablePath.parse("a..b..c.."));
        Assert.assertSame(VariablePath.parse("a.b.c"), VariablePath.parse("a.b.c"));
    }

    @Test
    public void rooted() {
        VariablePath vp = VariablePath.parse(".a.b.c");
        Assert.assertEquals(VariablePath.of(".", "a","b", "c"), vp);
        Assert.assertEquals("[.a.b.c]", vp.toString());
        Assert.assertTrue(groovyPattern.matcher(vp.groovyExpression()).matches());
    }

    @Test
    public void root() {
        VariablePath vp = VariablePath.parse(".");
        Assert.assertEquals(VariablePath.of("."), vp);
        Assert.assertEquals("[.]", vp.toString());
        Assert.assertTrue(groovyPattern.matcher(vp.groovyExpression()).matches());
    }

    @Test
    public void meta() {
        VariablePath vp = VariablePath.ofMeta("a");
        Assert.assertSame(vp, VariablePath.parse("#a"));
        Assert.assertFalse(vp.isTimestamp());
        Assert.assertTrue(vp.isMeta());
        Assert.assertEquals(VariablePath.ofMeta("a"), vp);
        Assert.assertEquals("[#a]", vp.toString());
        Assert.assertEquals("event.getMeta(\"a\")", vp.groovyExpression());
    }

    @Test
    public void timestamp() {
        Assert.assertTrue(VariablePath.TIMESTAMP.isTimestamp());
        Assert.assertFalse(VariablePath.TIMESTAMP.isMeta());
        Assert.assertEquals("[@timestamp]", VariablePath.TIMESTAMP.toString());
        Assert.assertEquals("event.getTimestamp()", VariablePath.TIMESTAMP.groovyExpression());
        Assert.assertEquals(VariablePath.TIMESTAMP, VariablePath.parse("@timestamp"));
    }

    @Test
    public void lastexception() {
        Assert.assertTrue(VariablePath.LASTEXCEPTION.isException());
        Assert.assertFalse(VariablePath.LASTEXCEPTION.isMeta());
        Assert.assertFalse(VariablePath.LASTEXCEPTION.isTimestamp());
        Assert.assertEquals("[@lastException]", VariablePath.LASTEXCEPTION.toString());
        Assert.assertEquals("event.getGroovyLastException()", VariablePath.LASTEXCEPTION.groovyExpression());
        Assert.assertEquals(VariablePath.LASTEXCEPTION, VariablePath.parse("@lastException"));
    }

}
