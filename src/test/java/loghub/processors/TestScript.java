package loghub.processors;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

import javax.script.ScriptEngineManager;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import loghub.Event;
import loghub.LogUtils;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Properties;

public class TestScript {

    private static Logger logger;

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.processors.Script");
        System.setProperty("python.home", folder.getRoot().getCanonicalPath());
    }

    @Test
    public void enumerateScript() {
        Properties props = new Properties(Collections.emptyMap());
        ScriptEngineManager factory = new ScriptEngineManager(props.classloader);
        factory.getEngineFactories().stream().forEach(i -> {
            Assert.assertNotNull(i.getScriptEngine());
            logger.debug("{}/{}: {}/{}", () -> i.getEngineName(), () -> i.getEngineVersion(), () -> i.getLanguageName(), () -> i.getLanguageVersion());
        });
    }

    @Test
    public void testJs() throws IOException, ProcessorException {
        Script s = new loghub.processors.Script();
        URL scripturl = getClass().getClassLoader().getResource("script.js");
        s.setScript(scripturl.getFile());
        Assert.assertTrue("Script engine for Javascript not found", s.configure(new Properties(Collections.emptyMap())));
        Event e = Tools.getEvent();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }

    @Test
    public void testPython() throws IOException, ProcessorException {
        Script s = new loghub.processors.Script();
        URL scripturl = getClass().getClassLoader().getResource("script.py");
        s.setScript(scripturl.getFile());
        Assert.assertTrue("Script engine for Python not found", s.configure(new Properties(Collections.emptyMap())));
        Event e = Tools.getEvent();
        s.process(e);
        Assert.assertTrue("event not transformed", (Boolean) e.get("done")); 
    }
}
