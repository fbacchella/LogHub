package loghub.configuration;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import loghub.Receiver;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Sender;
import loghub.transformers.Pipeline;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class Configuration {

    private Map<String, List<Pipeline>> pipelines = new HashMap<>();

    public Configuration() {
    }

    public void parse(String fileName) {
        CharStream cs;
        try {
            cs = new ANTLRFileStream(fileName);
        } catch (IOException e1) {
            throw new RuntimeException(e1.getMessage());
        }
        
        //Passing the input to the lexer to create tokens
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        //Passing the tokens to the parser to create the parse trea.
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        loghub.RouteParser.ConfigurationContext tree = parser.configuration(); // begin parsing at init rule

        ConfigListener conf = new ConfigListener();
        ParseTreeWalker walker = new ParseTreeWalker();
        try {
            walker.walk(conf, tree);
        } catch (ConfigException e) {
            throw new RuntimeException("Error at " + e.getStartPost() + ": " + e.getMessage(), e);
        }
        pipelines = conf.pipelines;        
    }

    public Set<Map.Entry<String, List<Pipeline>>> getTransformersPipe() {
        return pipelines.entrySet();
    }

    public Receiver[] getReceivers(String inEndpoint) {
//        List<Object> descriptions = slots.get(IN_SLOT);
//        Receiver[] receivers = new Receiver[descriptions.size()];
//        int i = 0;
//        for(Object o: descriptions) {
//            Receiver r = (Receiver) o;
//            r.configure(context, inEndpoint, eventQueue);
//            receivers[i++] = r;
//        }
//        return receivers;
        return new Receiver[0];
    }

    public Sender[] getSenders(String outEndpoint) {
//        List<Object> descriptions = slots.get(OUT_SLOT);
//        Sender[] senders = new Sender[descriptions.size()];
//        int i = 0;
//        for(Object oi: descriptions) {
//            Sender r = (Sender) oi;
//            r.configure(context, outEndpoint, eventQueue);
//            senders[i++] = r;
//        }
//
//        return senders;
        return new Sender[0];
    }

}
