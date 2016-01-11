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
    public void testUrlDecoder() {
        DecodeUrl t = new DecodeUrl();
        t.setFields(new Object[] { "*" });
        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(t);
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        t.process(e);
        Assert.assertEquals("key 'q' invalid", "\"Paints\" Oudalan", e.get("q"));
        Assert.assertEquals("key 'userAgent' not found", "%20", e.get("userAgent"));
    }

    @Test
    public void testUrlDecoderLoop() {
        DecodeUrl t = new DecodeUrl();
        t.setFields(new Object[] { "userAgent" });
        t.setLoop(true);
        EventWrapper e = new EventWrapper(new Event());
        e.setProcessor(t);
        e.put("q", "%22Paints%22+Oudalan");
        e.put("userAgent", "%2520");
        t.process(e);
        System.out.println(e);
        Assert.assertEquals("key 'q' invalid", "%22Paints%22+Oudalan", e.get("q"));
        Assert.assertEquals("key 'userAgent' not found", " ", e.get("userAgent"));
    }

}
