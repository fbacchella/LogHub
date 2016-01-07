package loghub.configuration;

import java.io.IOException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import loghub.LogUtils;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Tools;

public class TestParser {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.PipeStep","loghub.Pipeline", "loghub.configuration.Configuration");
    }

    @Test
    public void test1() throws Exception {
        org.antlr.v4.gui.TestRig.main(new String[] {
                "loghub.Route",
                "configuration",
                "-tokens",
                "-diagnostics",
                "-ps","routetry.ps",
                getClass().getClassLoader().getResource("test.conf").getFile()
        });
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        CharStream cs = new ANTLRInputStream(getClass().getClassLoader().getResourceAsStream("test.conf"));
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        loghub.RouteParser.ConfigurationContext tree = parser.configuration(); // begin parsing at init rule

        Assert.assertFalse(errListener.isFailed());
        ConfigListener conf = new ConfigListener();
        ParseTreeWalker walker = new ParseTreeWalker();
        try {
            walker.walk(conf, tree);
        } catch (ConfigException e) {
            logger.error("Error at " + e.getStartPost() + ": " + e.getMessage(), e);
            Assert.fail("parsing failed");
        }
        Assert.assertEquals("stack not empty :" + conf.stack, 0, conf.stack.size());

        for(String s: new String[] {"oneref", "main", "groovy"}) {
            Assert.assertTrue("pipeline " + s + " not found", conf.pipelines.containsKey(s));
        }
        Assert.assertEquals("too much pipelines", 3, conf.pipelines.size());
        Assert.assertEquals("too much inputs", 1, conf.inputs.size());
        Assert.assertEquals("too much outputs", 1, conf.outputs.size());
        for(String s: new String[] {"logfile", "plugins"}) {
            Assert.assertTrue("property " + s + " not found", conf.properties.containsKey(s));
        }
    }
}
