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
        Event e = Tools.getEvent();
        e.applyAtPath((i, j, k) -> i.put(j, k), new String[]{"a", "b", "c"}, 1, true);
        e.put("d", 2);
        e.applyAtPath((i, j, k) -> i.put(j, k), new String[]{"e"}, 3, true);
        Assert.assertEquals("wrong number of keys", 3, e.keySet().size());
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.applyAtPath((i, j, k) -> i.get(j), new String[]{"a", "b", "c"}, null));
        Assert.assertEquals("Didn't resolve the path correctly",  1, e.applyAtPath((i, j, k) -> i.remove(j), new String[]{"a", "b", "c"}, null));
        Assert.assertEquals("Didn't resolve the path correctly",  2, e.applyAtPath((i, j, k) -> i.get(j), new String[]{"d"}, null) );
        Assert.assertEquals("Didn't resolve the path correctly",  3, e.get("e") );
    }

}
