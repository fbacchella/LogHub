package loghub.processors;

import java.io.IOException;
import java.util.Collections;

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

public class TestUrlDecoders {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.DecodeUrl");
    }

    @Test
    public void testUrlDecoder() throws ProcessorException {
        DecodeUrl t = new DecodeUrl();
        t.setFields(new String[]{"*"});
        Event e = Tools.getEvent();
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("key 'q' invalid", "\"Paints\" Oudalan", e.get("q"));
        Assert.assertEquals("key 'userAgent' not found", "%20", e.get("userAgent"));
    }

    @Test
    public void testUrlDecoderLoop() throws ProcessorException {
        DecodeUrl t = new DecodeUrl();
        t.setFields(new String[]{"userAgent"});
        t.setLoop(true);
        Event e = Tools.getEvent();
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        Tools.runProcessing(e, "main", Collections.singletonList(t));
        Assert.assertEquals("key 'q' invalid", "%22Paints%22+Oudalan", e.get("q"));
        Assert.assertEquals("key 'userAgent' not found", " ", e.get("userAgent"));
    }

}
