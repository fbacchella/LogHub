package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.EventWrapper;
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
        e.setProcessor(t);
        e.put(".message", "a=1;b:2;c");
        t.process(e);
        System.out.println(rootEvent);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get(".message"));
    }

    @Test
    public void test2() throws ProcessorException {
        VarExtractor t = new VarExtractor();
        t.setField("message");
        t.setParser("(?<name>[a-z]+)[=:](?<value>[^;]+);?");
        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(t);
        e.put("message", "a=1;b:2");
        t.process(e);
        System.out.println(e);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b found", "2", e.get("b"));
        Assert.assertEquals("key message found", null, e.get("message"));
    }

    @Test
    public void test3() throws ProcessorException {
        VarExtractor t = new VarExtractor();
        t.setField("message");

        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(t);
        e.put("message", "a=1;b:2;c");
        t.process(e);
        Assert.assertEquals("key a not found", "1", e.get("a"));
        Assert.assertEquals("key b not found", "2", e.get("b"));
        Assert.assertEquals("key message not found", "c", e.get("message"));
    }

}
