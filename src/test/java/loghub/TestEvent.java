package loghub;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEvent {

    private static Logger logger ;


    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub");
    }

    @Test()
    public void TestPath() {
        Event e = new Event();
        e.put(new String[]{"a", "b", "c"}, 1);
        e.put("d", 2);
        e.put(new String[]{"e"}, 3);
        Assert.assertEquals("wrong number of keys", 3, e.keySet().size());
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.get(new String[]{"a", "b", "c"}) );
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.remove(new String[]{"a", "b", "c"}) );
        Assert.assertEquals("Didn't resolve the path correctly",  null, e.remove(new String[]{"a", "b", "c"}) );
        Assert.assertEquals("Didn't resolve the path correctly",  2, e.get(new String[]{"d"}) );
        Assert.assertEquals("Didn't resolve the path correctly",  3, e.get("e") );
    }

}
