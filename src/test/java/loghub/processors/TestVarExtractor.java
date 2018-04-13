package loghub.processors;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;

public class TestVarExtractor {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.VarExtractor");
    }

    @Test
    public void test1() throws ProcessorException {
        VarExtractor t = new VarExtractor();
        t.setPath("sub");
        t.setField(".message");
        t.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        Event e = Tools.getEvent();
        e.put("message", "a=1;b:2;c");
        Assert.assertTrue(e.process(t));
        @SuppressWarnings("unchecked")
        Map<String, Object> sub = (Map<String, Object>) e.get("sub");
        Assert.assertEquals("key a not found", "1", sub.get("a"));
        Assert.assertEquals("key b not found", "2", sub.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));
    }

    @Test
    public void test2() throws ProcessorException {
        VarExtractor t = new VarExtractor();
        t.setField("message");
        t.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        Event e = Tools.getEvent();
        e.put("message", "a=1;b:2");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b found", "2", e.get("b"));
        Assert.assertNull("key message found", e.get("message"));
    }

    @Test
    public void test3() throws ProcessorException {
        VarExtractor t = new VarExtractor();
        t.setField("message");

        Event e = Tools.getEvent();
        e.put("message", "a=1;b:2;c");
        e.process(t);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));
    }

}
