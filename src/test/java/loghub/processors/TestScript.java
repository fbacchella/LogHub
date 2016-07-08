package loghub.processors;

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
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;
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
    public void testJs() throws IOException, ProcessorException {
        Script s = new loghub.processors.Script();
        URL scripturl = getClass().getClassLoader().getResource("script.js");
        s.setScript(scripturl.getFile());
        s.configure(new Properties(Collections.emptyMap()));
        Event e = Tools.getEvent();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }

    @Test
    public void testPython() throws IOException, ProcessorException {
        Script s = new loghub.processors.Script();
        s.setScript("script.py");
        s.configure(new Properties(Collections.emptyMap()));
        Event e = Tools.getEvent();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }
}
