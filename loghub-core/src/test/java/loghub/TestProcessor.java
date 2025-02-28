package loghub;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.processors.Identity;

public class TestProcessor {

    private final EventsFactory factory = new EventsFactory();

    @BeforeClass
    public static void configure() {
        Tools.configure();
        Logger logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.netty");
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

        p.setIf(new Expression(true));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(false));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(new Expression(0));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf(new Expression(1));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(0.1));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression("bob"));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf(new Expression(""));
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));
    }

}
