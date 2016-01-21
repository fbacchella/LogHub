package loghub.configuration;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers.ThrowingConsumer;
import loghub.Helpers.ThrowingFunction;
import loghub.Helpers.ThrowingPredicate;
import loghub.NamedArrayBlockingQueue;
import loghub.PipeStep;
import loghub.Pipeline;
import loghub.Processor;
import loghub.Receiver;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Sender;
import loghub.configuration.ConfigListener.Input;
import loghub.configuration.ConfigListener.ObjectReference;
import loghub.configuration.ConfigListener.Output;
import loghub.processors.PipeRef;
import loghub.processors.SubPipeline;
import loghub.processors.Test;

public class Configuration {

    public static final class PipeJoin {
        public final String inpipe;
        public final String outpipe;
        PipeJoin(String inpipe, String outpipe) {
            this.inpipe = inpipe;
            this.outpipe = outpipe;
        }
        @Override
        public String toString() {
            return inpipe + "->" + outpipe;
        }
    }

    private static final Logger logger = LogManager.getLogger();
    private static final ThrowingFunction<Class<Object>, Object> emptyConstructor = i -> {return i.getConstructor().newInstance();};

    public Map<String, Pipeline> namedPipeLine = null;
    public Set<Pipeline> pipelines = new HashSet<>();
    public Set<PipeJoin> joins = null;
    private List<Receiver> receivers;
    Set<String> inputpipelines = new HashSet<>();
    Set<String> outputpipelines = new HashSet<>();
    private List<Sender> senders;
    public Properties properties;
    private ClassLoader classLoader = Configuration.class.getClassLoader();

    public Configuration() {
    }

    public void parse(String fileName) {
        CharStream cs;
        try {
            cs = new ANTLRFileStream(fileName);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        //Passing the input to the lexer to create tokens
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        //Passing the tokens to the parser to create the parse tree.
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        loghub.RouteParser.ConfigurationContext tree = parser.configuration();
        if(errListener.failed) {
            throw new RuntimeException("parsing failed");
        }

        ConfigListener conf = new ConfigListener();
        ParseTreeWalker walker = new ParseTreeWalker();
        try {
            walker.walk(conf, tree);
        } catch (ConfigException e) {
            throw new RuntimeException("Error at " + e.getStartPost() + ": " + e.getMessage(), e);
        }

        namedPipeLine = new HashMap<>(conf.pipelines.size());

        Function<Object, Object> resolve = i -> ((i instanceof ConfigListener.ObjectWrapped) ? ((ConfigListener.ObjectWrapped) i).wrapped : 
            (i instanceof ConfigListener.ObjectReference) ? parseObjectDescription((ConfigListener.ObjectDescription) i, emptyConstructor) : i);

        final Map<String, Object > newProperties = new HashMap<>(conf.properties.size());
        conf.properties.entrySet().stream().forEach( i-> newProperties.put(i.getKey(), resolve.apply(i.getValue())));

        // Neeeded because conf.properties store a lot of wrapped object, they needs to be resolved
        if(newProperties.containsKey("plugins") && newProperties.get("plugins") instanceof List) {
            try {
                @SuppressWarnings("unchecked")
                List<Object> plugins = (List<Object>) newProperties.remove("plugins");
                classLoader = doClassLoader(plugins);
            } catch (IOException ex) {
                throw new RuntimeException("can't load plugins: " + ex.getMessage(), ex);
            }
        }

        newProperties.put(Properties.CLASSLOADERNAME, classLoader);
        newProperties.put(Properties.NAMEDPIPELINES, namedPipeLine);
        newProperties.put(Properties.FORMATTERS, conf.formatters);

        properties = new Properties(newProperties);

        // Generate all the named pipeline
        for(Entry<String, ConfigListener.Pipeline> e: conf.pipelines.entrySet()) {
            List<Pipeline> currentPipeList = new ArrayList<>();
            String name = e.getKey();
            Pipeline p = parsePipeline(e.getValue(), name, currentPipeList, 0);
            namedPipeLine.put(name, p);
        }
        namedPipeLine = Collections.unmodifiableMap(namedPipeLine);
        pipelines = Collections.unmodifiableSet(pipelines);

        // Fill the receivers list
        receivers = new ArrayList<>();
        for(Input i: conf.inputs) {
            if(i.piperef == null || ! namedPipeLine.containsKey(i.piperef)) {
                throw new RuntimeException("Invalid input, no destination pipeline: " + i);
            }
            for(ConfigListener.ObjectDescription desc: i.receiver) {
                Pipeline p = namedPipeLine.get(i.piperef);
                ThrowingFunction<Class<Receiver>, Receiver> receiverConstructor = r -> {return r.getConstructor(NamedArrayBlockingQueue.class).newInstance(p.inQueue);};
                Receiver r = (Receiver) parseObjectDescription(desc, receiverConstructor);
                //logger.debug("receiver {} destination point will be {}", () -> i, () -> namedPipeLine.get(i.piperef).inQueue);
                receivers.add(r);
            }
            inputpipelines.add(i.piperef);
        }
        inputpipelines = Collections.unmodifiableSet(inputpipelines);
        receivers = Collections.unmodifiableList(receivers);

        // Fill the senders list
        senders = new ArrayList<>();
        for(Output o: conf.outputs) {
            if(o.piperef == null || ! namedPipeLine.containsKey(o.piperef)) {
                throw new RuntimeException("Invalid output, no source pipeline: " + o);
            }
            for(ConfigListener.ObjectDescription desc: o.sender) {
                Pipeline p = namedPipeLine.get(o.piperef);
                ThrowingFunction<Class<Sender>, Sender> senderConstructor = r -> {return r.getConstructor(NamedArrayBlockingQueue.class).newInstance(p.outQueue);};
                Sender s = (Sender) parseObjectDescription(desc, senderConstructor);
                //logger.debug("sender {} source point will be {}", () -> s, () -> namedPipeLine.get(o.piperef).outQueue);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
        outputpipelines = Collections.unmodifiableSet(outputpipelines);
        senders = Collections.unmodifiableList(senders);

        joins = Collections.unmodifiableSet(conf.joins);
    }

    private Pipeline parsePipeline(ConfigListener.Pipeline desc, String currentPipeLineName, List<Pipeline> currentPipeList, int depth) {
        List<PipeStep[]> pipeList = new ArrayList<PipeStep[]>() {
            @Override
            public String toString() {
                StringBuilder buffer = new StringBuilder();
                buffer.append("PipeList(");
                for(PipeStep[] i: this) {
                    buffer.append(Arrays.toString(i));
                    buffer.append(", ");
                }
                buffer.setLength(buffer.length() - 2);
                buffer.append(')');
                return buffer.toString();
            }
        };

        int threads = -1;
        int rank = 0;
        PipeStep[] steps = null;
        for(ConfigListener.Processor i: desc.processors) {
            Processor t = getProcessor(i, currentPipeLineName, currentPipeList, depth);
            if(t.getThreads() != threads) {
                threads = t.getThreads();
                steps = new PipeStep[threads];
                pipeList.add(steps);
                rank++;
                for(int j =0; j < threads ; j++) {
                    steps[j] = new PipeStep(currentPipeLineName + "." + depth, rank, j + 1);
                }
            }
            for(int j = 0; j < threads ; j++) {
                steps[j].addProcessor(t);
            } 
        }
        Pipeline pipe = new Pipeline(pipeList, currentPipeLineName + "$" + currentPipeList.size());
        currentPipeList.add(pipe);
        pipelines.add(pipe);
        return pipe;
    }

    private Processor getProcessor(ConfigListener.Processor i, String currentPipeLineName, List<Pipeline> currentPipeList, int depth) {
        Processor t;
        if(i instanceof ConfigListener.ProcessorInstance) {
            ConfigListener.ProcessorInstance ti = (ConfigListener.ProcessorInstance) i;
            t = (Processor) parseObjectDescription(ti, emptyConstructor);
        } else if (i instanceof ConfigListener.Test){
            ConfigListener.Test ti = (ConfigListener.Test) i;
            Test test = new Test();
            test.setIf(ti.test);
            test.setThen(getProcessor(ti.True, currentPipeLineName, currentPipeList, depth + 1));
            if(ti.False != null) {
                test.setElse(getProcessor(ti.False, currentPipeLineName, currentPipeList, depth + 1));
            }
            t = test;
        } else if (i instanceof ConfigListener.PipeRef){
            ConfigListener.PipeRef cpr = (ConfigListener.PipeRef) i;
            PipeRef pr = new PipeRef();
            pr.setPipeRef(cpr.pipename);
            t = pr;
        } else if (i instanceof ConfigListener.Pipeline){
            ConfigListener.Pipeline pl = (ConfigListener.Pipeline) i;
            Pipeline pipe = parsePipeline(pl, currentPipeLineName, currentPipeList, depth + 1);
            t = new SubPipeline(pipe);
        } else {
            throw new RuntimeException("unknown configuration element: " + i);
        }
        return t;
    }

    private <T, C> T parseObjectDescription(ConfigListener.ObjectDescription desc, ThrowingFunction<Class<T>, T> constructor) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) classLoader.loadClass(desc.clazz);
            T object = constructor.apply(clazz);

            for(Entry<String, ObjectReference> i: desc.beans.entrySet()) {
                ObjectReference ref = i.getValue();
                Object beanValue;
                if(i.getValue() instanceof ConfigListener.ObjectWrapped) {
                    beanValue = ((ConfigListener.ObjectWrapped) ref).wrapped;
                } else if (i.getValue() instanceof ConfigListener.ObjectDescription) {
                    beanValue = parseObjectDescription((ConfigListener.ObjectDescription) ref, emptyConstructor);
                } else {
                    throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz), desc.ctx.start, desc.ctx.stop);
                }
                try {
                    BeansManager.beanSetter(object, i.getKey(), beanValue);
                } catch (InvocationTargetException ex) {
                    throw new ConfigException(String.format("Invalid bean '%s.%s': %s", desc.clazz, i.getKey(), ex.getCause()), desc.ctx.start, desc.ctx.stop, (Exception) ex.getCause());
                }
            }
            return object;
        } catch (ClassNotFoundException e) {
            throw new ConfigException(String.format("Unknown class '%s'", desc.clazz), desc.ctx.start, desc.ctx.stop);
        } catch (ConfigException e) {
            throw e;
        } catch (RuntimeException | ExceptionInInitializerError e) {
            Exception rootCause = ( Exception) e;
            if(rootCause.getCause() instanceof InvocationTargetException) {
                rootCause = (InvocationTargetException) rootCause.getCause();
            }
            logger.throwing(rootCause);
            throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz, rootCause), desc.ctx.start, desc.ctx.stop);
        }
    }

    ClassLoader doClassLoader(List<Object> pathElements) throws IOException {

        final Collection<URL> urls = new ArrayList<URL>();

        // Needed for all the lambda that throws exception
        ThrowingPredicate<Path> filterReadable = i -> ! Files.isHidden(i);
        ThrowingConsumer<Path> toUrl = i -> urls.add(i.toUri().toURL());

        pathElements.stream()
        .map(i -> Paths.get(i.toString()))
        .filter(i -> Files.isReadable(i))
        .filter(filterReadable)
        .filter(i -> (Files.isRegularFile(i) && i.toString().endsWith(".jar")) || Files.isDirectory(i))
        .forEach( i-> {
            toUrl.accept(i);
            if(Files.isDirectory(i)) {
                try {
                    Files.list(i)
                    .filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".jar"))
                    .forEach(toUrl);
                } catch (Exception e) {
                    new RuntimeException(e);
                }
            }
        });

        //Add myself to class loader, so the custom class loader is used in priority
        Path myself = locateResourcefile("loghub");
        if(myself.endsWith("loghub")) {
            myself = myself.getParent();
        }
        urls.add(myself.toUri().toURL());

        return new URLClassLoader(urls.toArray(new URL[] {}), getClass().getClassLoader()) {
            @Override
            public String toString() {
                return "Loghub's class loader";
            }
        };
    }

    private Path locateResourcefile(String ressource) {
        try {
            URL url = getClass().getClassLoader().getResource(ressource);
            String protocol = url.getProtocol();
            String file = null;
            switch(protocol) {
            case "file":
                file = url.getFile();
                break;
            case "jar":
                url = new URL(url.getFile().replaceFirst("!.*", ""));
                file = url.getFile();
                break;
            default:
                throw new IllegalArgumentException("unmanaged ressource URL");
            }
            return Paths.get(file);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("can't locate the ressource file path", e);
        }
    }

    public Collection<Receiver> getReceivers() {
        return Collections.unmodifiableList(receivers);
    }

    public Collection<Sender> getSenders() {
        return Collections.unmodifiableList(senders);
    }

}
