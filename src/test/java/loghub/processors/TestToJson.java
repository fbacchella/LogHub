package loghub.processors;

import java.io.IOException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;

public class TestToJson {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.transformers.Script");
    }

    @Test
    public void test() throws ProcessorException {
        Event e = Tools.getEvent();
        Processor t = new ParseJson();
        e.put("message", "{\"@timestamp\": \"2019-10-04T16:36:31.406Z\", \"a\": 1}");
        e.process(t);
        Assert.assertEquals("2019-10-04T16:36:31.406Z", e.get("_@timestamp"));
        Assert.assertEquals(1, e.get("a"));
    }

}
