package loghub.configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import loghub.Pipeline;
import loghub.Receiver;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Sender;
import loghub.configuration.ConfigListener.Input;
import loghub.configuration.ConfigListener.Output;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

public class Configuration {

    public Map<String, List<Pipeline>> pipelines = null;
    public Map<String, Pipeline> namedPipeLine = null;
    private List<Receiver> receivers;
    private Set<String> inputpipelines = new HashSet<>();
    private Set<String> outputpipelines = new HashSet<>();
    private List<Sender> senders;
    
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
        pipelines = Collections.unmodifiableMap(conf.pipelines);
        namedPipeLine = new HashMap<>(pipelines.size());
        for(Map.Entry<String, List<Pipeline>> e: pipelines.entrySet()) {
            namedPipeLine.put(e.getKey(), e.getValue().get(e.getValue().size() - 1));
        }
        namedPipeLine = Collections.unmodifiableMap(namedPipeLine);
        
        // File the receivers list
        receivers = new ArrayList<>();
        for(Input i: conf.inputs) {
            if(i.piperef == null || ! namedPipeLine.containsKey(i.piperef)) {
                throw new RuntimeException("Invalid input, no destination pipeline: " + i);
            }
            for(Receiver r: i.receiver) {
                System.out.println("receiver " + i + " destination point will be " + namedPipeLine.get(i.piperef).inEndpoint);
                r.setEndpoint(namedPipeLine.get(i.piperef).inEndpoint);
                receivers.add(r);
            }
            inputpipelines.add(i.piperef);
        }
        
        // File the senders list
        senders = new ArrayList<>();
        for(Output o: conf.outputs) {
            if(o.piperef == null || ! namedPipeLine.containsKey(o.piperef)) {
                throw new RuntimeException("Invalid output, no source pipeline: " + o);
            }
            for(Sender s: o.sender) {
                System.out.println("sender " + s + " source point will be " + namedPipeLine.get(o.piperef).outEndpoint);
                s.setEndpoint(namedPipeLine.get(o.piperef).outEndpoint);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
    }

    public Set<Map.Entry<String, List<Pipeline>>> getTransformersPipe() {
        return pipelines.entrySet();
    }

    public Collection<String> getReceiversPipelines() {
        return Collections.unmodifiableSet(inputpipelines);
    }

    public Collection<Receiver> getReceivers() {
        return Collections.unmodifiableList(receivers);
    }

    public Collection<Sender> getSenders() {
        return Collections.unmodifiableList(senders);
    }

    public Collection<String> getSendersPipelines() {
        return Collections.unmodifiableSet(outputpipelines);
    }

}
