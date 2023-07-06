package loghub.configuration;

import java.io.IOException;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import io.netty.util.CharsetUtil;
import loghub.LogUtils;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Tools;
import loghub.configuration.ConfigListener.ProcessorInstance;

public class TestParser {

    private static Logger logger;

    @BeforeClass
    static public void configure() throws IOException {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.TRACE, "loghub.SmartContext", "loghub.PipeStep","loghub.Pipeline", "loghub.configuration.Configuration");
    }

    @Ignore
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

    @Ignore
    @Test
    public void test2() throws IOException {
        CharStream cs = CharStreams.fromStream(getClass().getClassLoader().getResourceAsStream("test.conf"), CharsetUtil.UTF_8);
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        loghub.RouteParser.ConfigurationContext tree = parser.configuration(); // begin parsing at init rule

        ConfigListener conf = ConfigListener.builder().build();
        conf.startWalk(tree, cs, parser);
        Assert.assertEquals("stack not empty :" + conf.stack, 0, conf.stack.size());
        for(String s: new String[] {"oneref", "main", "groovy"}) {
            Assert.assertTrue("pipeline " + s + " not found", conf.pipelines.containsKey(s));
        }
        Assert.assertEquals("too much pipelines", 6, conf.pipelines.size());
        Assert.assertEquals("too much inputs", 1, conf.inputs.size());
        Assert.assertEquals("too much outputs", 1, conf.outputs.size());
        for(String s: new String[] {"logfile", "plugins"}) {
            Assert.assertTrue("property " + s + " not found", conf.properties.containsKey(s));
        }
    }

    @Ignore
    @Test
    public void testType() throws IOException {
        CharStream cs = CharStreams.fromStream(getClass().getClassLoader().getResourceAsStream("types.conf"), CharsetUtil.UTF_8);
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        loghub.RouteParser.ConfigurationContext tree = parser.configuration(); // begin parsing at init rule

        ConfigListener conf = ConfigListener.builder().build();
        conf.startWalk(tree, cs, parser);
        Assert.assertEquals("stack not empty :" + conf.stack, 0, conf.stack.size());
        ConfigListener.PipenodesList main = conf.pipelines.get("main");
        ProcessorInstance p = (ProcessorInstance) main.processors.get(0);
        //Assert.assertArrayEquals(VariablePath.of(new Object[]{"0", 1, 1.0, true}), p.wrapped.getPathArray());
    }

}
