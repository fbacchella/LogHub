package loghub.transformers;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;

public class TestVarExtractor {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.transformers.Script");
    }

    @Test
    public void test1() {
        Event e = new Event();
        e.put("message", "a=1;b:2;c");
        VarExtractor t = new VarExtractor();
        t.setField("message");
        t.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        t.transform(e);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));

    }

    @Test
    public void test2() {
        Event e = new Event();
        e.put("message", "a=1;b:2");
        VarExtractor t = new VarExtractor();
        t.setField("message");
        t.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        t.transform(e);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b found", "2", e.get("b"));
        Assert.assertEquals("key message found", null, e.get("message"));

    }

    @Test
    public void test3() {
        Event e = new Event();
        e.put("message", "a=1;b:2;c");
        VarExtractor t = new VarExtractor();
        t.setField("message");
        t.transform(e);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));

    }
}
