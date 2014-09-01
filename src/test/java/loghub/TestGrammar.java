package loghub;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.configuration.ConfigErrorListener;
import loghub.configuration.ConfigException;
import loghub.configuration.ConfigListener;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class TestGrammar {

    private static final Logger logger = LogManager.getLogger();

    @Test
    public void test1() throws Exception {
        org.antlr.v4.runtime.misc.TestRig.main(new String[] {
                "loghub.Route",
                "configuration",
                "-tokens",
                "-diagnostics",
                "-ps","routetry.ps",
                getClass().getClassLoader().getResource("test.conf").getFile()
        });
    }

    @Test
    public void test2() throws IOException {
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
            System.out.println("Error at " + e.getStartPost() + ": " + e.getMessage());
            e.printStackTrace();
            if(e.getCause() != null) {
                e.getCause().printStackTrace();                
            }
        }

        System.out.println("pipelines");
        System.out.println(conf.pipelines);
        //        for(Map.Entry<String, List<Pipeline>> i: conf.pipelines.entrySet()) {
        //            System.out.println(i.getKey());
        //            for(Pipeline j: i.getValue()) {
        //                System.out.println("   " + j);
        //            }
        //        }
        System.out.println("stack");
        System.out.println(conf.stack);
        System.out.println("inputs");
        System.out.println(conf.inputs);
        System.out.println("outputs");
        System.out.println(conf.outputs);
        Map<byte[], Event> eventQueue = new ConcurrentHashMap<>();
        Map<String, Pipeline> pipelines = new HashMap<>();
        for(Map.Entry<String, List<Pipeline>> e: conf.pipelines.entrySet()) {
            Pipeline last = null;
            for(Pipeline p: e.getValue()) {
                p.startStream(eventQueue);
                System.out.println(p.getInEndpoint() + "->" + p.getOutEndpoint());
                last = p;
            }
            if(last != null) {
                pipelines.put(e.getKey(), last);
            }
        }
        ZMQManager.terminate();
    }
}
