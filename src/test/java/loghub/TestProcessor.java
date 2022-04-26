package loghub;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.configuration.Properties;
import loghub.processors.Identity;

public class TestProcessor {

    private Expression getExpression(String expressionScript) throws Expression.ExpressionException {
        return new Expression(expressionScript, new Properties(Collections.emptyMap()).groovyClassLoader, Collections.emptyMap());
    }

    @Test
    public void testPath() {
        Processor p = new Processor() {

            @Override
            public boolean process(Event event) {
                return true;
            }

            @Override
            public String getName() {
                return null;
            }
        };
        p.setPath("a.b.c");
        Assert.assertEquals("Prefix don't match ", "[a.b.c]", p.getPath());
        p.setPath("");
        Assert.assertEquals("Prefix don't match ", "[]", p.getPath());
        p.setPath(".a");
        Assert.assertEquals("Prefix don't match ", "[.a]", p.getPath());
    }

    @Test
    public void testIf() throws ProcessorException, Expression.ExpressionException {
        Event e = new EventInstance(ConnectionContext.EMPTY);

        Processor p = new Identity();

        p.setIf(getExpression("true"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(getExpression("false"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(getExpression("0"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(getExpression("1"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(getExpression("0.1"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(getExpression("\"bob\""));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(getExpression("\"\""));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));
    }

}
