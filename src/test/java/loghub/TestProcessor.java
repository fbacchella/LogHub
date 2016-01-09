package loghub;

import org.junit.Assert;
import org.junit.Test;

public class TestProcessor {

    @Test
    public void testPath() {
        Processor p = new Processor() {

            @Override
            public void process(Event event) {
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

}
