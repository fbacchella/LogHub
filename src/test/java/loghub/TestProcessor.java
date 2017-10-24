package loghub;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import loghub.configuration.Properties;
import loghub.processors.Identity;

public class TestProcessor {

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
        Assert.assertEquals("Prefix don't match ", "a.b.c", p.getPath());
        p.setPath("");
        Assert.assertEquals("Prefix don't match ", "", p.getPath());
    }

    @Test
    public void testIf() throws ProcessorException {
        Event e = new EventInstance(ConnectionContext.EMPTY);

        Processor p = new Identity();

        p.setIf("true");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf("false");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf("0");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));

        p.setIf("1");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf("0.1");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf("\"bob\"");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertTrue(p.isprocessNeeded(e));

        p.setIf("\"\"");
        p.configure(new Properties(Collections.emptyMap()));
        Assert.assertFalse(p.isprocessNeeded(e));
    }

}
