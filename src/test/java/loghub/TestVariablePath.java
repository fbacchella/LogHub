package loghub;

import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class TestVariablePath {
    
    @Test
    public void single() {
        VariablePath vp = VariablePath.of("a");
        Assert.assertEquals(vp, VariablePath.of(new String[]{"a"}));
        Assert.assertEquals(vp, VariablePath.of(Collections.singletonList("a")));
        Assert.assertEquals("[a]", vp.toString());
        Assert.assertEquals("event.getGroovyPath(\"a\")", vp.groovyExpression());
    }

    @Test
    public void empty() {
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of(""));
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of(" "));
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of(new String[]{}));
        Assert.assertSame(VariablePath.EMPTY, VariablePath.of(Collections.emptyList()));
        Assert.assertEquals("[]", VariablePath.EMPTY.toString());
        Assert.assertEquals("event", VariablePath.EMPTY.groovyExpression());
    }

    private void checkABC(VariablePath vp) {
        Assert.assertFalse(vp.isTimestamp());
        Assert.assertFalse(vp.isMeta());
        Assert.assertFalse(vp.isIndirect());
        Assert.assertEquals(VariablePath.of(new String[]{"a", "b", "c"}), vp);
        Assert.assertEquals(VariablePath.of(List.of("a","b", "c")), vp);
        Assert.assertEquals("[a.b.c]", vp.toString());
        Assert.assertEquals("event.getGroovyPath(\"a\",\"b\",\"c\")", vp.groovyExpression());
    }

    @Test
    public void variantABC() {
        checkABC(VariablePath.of("a.b.c"));
        checkABC(VariablePath.of("a..b..c."));
        checkABC(VariablePath.of("a..b..c.."));
        Assert.assertSame(VariablePath.of("a.b.c"), VariablePath.of("a.b.c"));
    }

    @Test
    public void rooted() {
        VariablePath vp = VariablePath.of(".a.b.c");
        Assert.assertEquals(VariablePath.of(new String[]{".", "a","b", "c"}), vp);
        Assert.assertEquals("[.a.b.c]", vp.toString());
        Assert.assertEquals("event.getGroovyPath(\".\",\"a\",\"b\",\"c\")", vp.groovyExpression());
    }

    @Test
    public void meta() {
        VariablePath vp = VariablePath.ofMeta("a");
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
    }

}
