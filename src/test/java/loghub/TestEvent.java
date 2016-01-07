package loghub;

import java.io.IOException;
import java.util.Map;

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

    @SuppressWarnings("unchecked")
    @Test()
    public void TestPath() {
        Event e = new Event();
        e.put("0", "a.b.c", 1);
        e.put("", "d", 2);
        Map<String, Object> current = e;
        System.out.println(e);
        for(String key: new String[] {"0", "a", "b"}) {
            current = (Map<String, Object>) current.get(key);
        }
        Assert.assertEquals("Didn't resolve the path correctly",  1, current.get("c") );
        System.out.println(e);
    }

}
