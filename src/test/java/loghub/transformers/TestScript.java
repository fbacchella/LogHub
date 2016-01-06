package loghub.transformers;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.Event;
import loghub.LogUtils;
import loghub.Tools;
import loghub.processors.Script;

public class TestScript {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.transformers.Script");
    }

    @Test
    public void testJs() throws IOException {
        Script s = new loghub.processors.Script();
        URL scripturl = getClass().getClassLoader().getResource("script.js");
        s.setScript(scripturl.getFile());
        s.configure(Collections.emptyMap());
        Event e = new Event();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }

    @Test
    public void testPython() throws IOException {
        Script s = new loghub.processors.Script();
        s.setScript("script.py");
        s.configure(Collections.emptyMap());
        Event e = new Event();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }
}
