package loghub;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Identity;

public class TestProcessor {

    private final EventsFactory factory = new EventsFactory();

    private Expression getExpression(String expressionScript) {
        return Tools.parseExpression(expressionScript);
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
        Assert.assertEquals("Prefix don't match ", VariablePath.of("a", "b", "c"), p.getPathArray());
        p.setPath("");
        Assert.assertEquals("Prefix don't match ", VariablePath.EMPTY, p.getPathArray());
        p.setPath(".a");
        Assert.assertEquals("Prefix don't match ", VariablePath.parse(".a"), p.getPathArray());
    }

    @Test
    public void testIf() throws ProcessorException {
        Event e = factory.newEvent();

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
