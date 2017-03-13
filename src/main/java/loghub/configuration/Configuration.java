package loghub.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import loghub.Event;
import loghub.Helpers.ThrowingConsumer;
import loghub.Helpers.ThrowingFunction;
import loghub.Helpers.ThrowingPredicate;
import loghub.Pipeline;
import loghub.Processor;
import loghub.Receiver;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Sender;
import loghub.configuration.ConfigListener.Input;
import loghub.configuration.ConfigListener.ObjectReference;
import loghub.configuration.ConfigListener.Output;
import loghub.processors.AnonymousSubPipeline;
import loghub.processors.NamedSubPipeline;

public class Configuration {

    private static final int DEFAULTQUEUEDEPTH = 100;

    private static final ThrowingFunction<Class<Object>, Object> emptyConstructor = i -> {return i.getConstructor().newInstance();};

    private List<Receiver> receivers;
    private Set<String> inputpipelines = new HashSet<>();
    private Set<String> outputpipelines = new HashSet<>();
    // Stores all the top level pipelines, that generate metrics
    private final Set<String> topPipelines = new HashSet<>();
    private List<Sender> senders;
    private ClassLoader classLoader = Configuration.class.getClassLoader();

    Configuration() {
    }

    public static Properties parse(String fileName) throws IOException, ConfigException {
        Configuration conf = new Configuration();
        return conf.runparsing(new ANTLRFileStream(fileName));
    }

    public static Properties parse(InputStream is) throws IOException, ConfigException {
        Configuration conf = new Configuration();
        return conf.runparsing(new ANTLRInputStream(is));
    }

    public static Properties parse(Reader r) throws ConfigException, IOException {
        Configuration conf = new Configuration();
        return conf.runparsing(new ANTLRInputStream(r));
    }

    private Properties runparsing(CharStream cs) throws IOException, ConfigException {
        Configuration conf = new Configuration();
        ConfigListener listener = conf.antlrparsing(cs);
        return conf.analyze(listener);
    }

    private ConfigListener antlrparsing(CharStream cs) throws IOException, ConfigException{

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
            throw errListener.exception;
        }

        ConfigListener conf = new ConfigListener();
        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(conf, tree);
        return conf;
    }

    private Properties analyze(ConfigListener conf) throws ConfigException {

        final Map<String, Object> newProperties = new HashMap<String, Object>(conf.properties.size() + Properties.PROPSNAMES.values().length + System.getProperties().size());

        // Resolvers properties found and and it to new properties
        Function<Object, Object> resolve = i -> {
            return ((i instanceof ConfigListener.ObjectWrapped) ? ((ConfigListener.ObjectWrapped) i).wrapped : 
                (i instanceof ConfigListener.ObjectReference) ? parseObjectDescription((ConfigListener.ObjectDescription) i, emptyConstructor) : i);
        };
        conf.properties.entrySet().stream().forEach( i-> newProperties.put(i.getKey(), resolve.apply(i.getValue())));

        Map<String, Pipeline> namedPipeLine = new HashMap<>(conf.pipelines.size());

        if (newProperties.containsKey("plugins") && newProperties.get("plugins").getClass().isArray() ) {
            try {
                Object[] plugins = (Object[]) newProperties.remove("plugins");
                classLoader = doClassLoader(plugins);
            } catch (IOException ex) {
                throw new RuntimeException("can't load plugins: " + ex.getMessage(), ex);
            }
        }
        newProperties.put(Properties.PROPSNAMES.CLASSLOADERNAME.toString(), classLoader);

        Set<Pipeline> pipelines = new HashSet<>();
        // Generate all the named pipeline
        for(Entry<String, ConfigListener.PipenodesList> e: conf.pipelines.entrySet()) {
            String name = e.getKey(); 
            Pipeline p = parsePipeline(e.getValue(), name, 0, new AtomicInteger());
            pipelines.add(p);
            namedPipeLine.put(name, p);
            if (p.nextPipeline != null) {
                topPipelines.add(p.nextPipeline);
            }
        }
        newProperties.put(Properties.PROPSNAMES.PIPELINES.toString(), Collections.unmodifiableSet(pipelines));
        namedPipeLine = Collections.unmodifiableMap(namedPipeLine);
        newProperties.put(Properties.PROPSNAMES.NAMEDPIPELINES.toString(), namedPipeLine);

        //Find the queue depth
        final int queuesDepth = newProperties.containsKey("queueDepth") ? (Integer) newProperties.remove("queueDepth") : DEFAULTQUEUEDEPTH;
        newProperties.put(Properties.PROPSNAMES.QUEUESDEPTH.toString(), queuesDepth);

        BlockingQueue<Event> mainQueue = new ArrayBlockingQueue<Event>(queuesDepth);
        Map<String, BlockingQueue<Event>> outputQueues = new HashMap<>(namedPipeLine.size());
        namedPipeLine.keySet().stream().forEach( i-> outputQueues.put(i, new ArrayBlockingQueue<Event>(queuesDepth)));

        newProperties.put(Properties.PROPSNAMES.FORMATTERS.toString(), conf.formatters);
        newProperties.put(Properties.PROPSNAMES.MAINQUEUE.toString(), mainQueue);
        newProperties.put(Properties.PROPSNAMES.OUTPUTQUEUE.toString(), outputQueues);

        // Fill the receivers list
        receivers = new ArrayList<>();
        for(Input i: conf.inputs) {
            if(i.piperef == null || ! namedPipeLine.containsKey(i.piperef)) {
                throw new RuntimeException("Invalid input, no destination pipeline: " + i);
            }
            for(ConfigListener.ObjectDescription desc: i.receiver) {
                Pipeline p = namedPipeLine.get(i.piperef);
                ThrowingFunction<Class<Receiver>, Receiver> receiverConstructor = r -> {return r.getConstructor(BlockingQueue.class, Pipeline.class).newInstance(mainQueue, p);};
                Receiver r = (Receiver) parseObjectDescription(desc, receiverConstructor);
                receivers.add(r);
            }
            inputpipelines.add(i.piperef);
        }
        topPipelines.addAll(inputpipelines);
        inputpipelines = Collections.unmodifiableSet(inputpipelines);
        receivers = Collections.unmodifiableList(receivers);
        newProperties.put(Properties.PROPSNAMES.RECEIVERS.toString(), receivers);

        // Fill the senders list
        senders = new ArrayList<>();
        for(Output o: conf.outputs) {
            if(o.piperef == null || ! namedPipeLine.containsKey(o.piperef)) {
                throw new RuntimeException("Invalid output, no source pipeline: " + o);
            }
            for(ConfigListener.ObjectDescription desc: o.sender) {
                BlockingQueue<Event> out = outputQueues.get(o.piperef);
                ThrowingFunction<Class<Sender>, Sender> senderConstructor = r -> {return r.getConstructor(BlockingQueue.class).newInstance(out);};
                Sender s = (Sender) parseObjectDescription(desc, senderConstructor);
                //logger.debug("sender {} source point will be {}", () -> s, () -> namedPipeLine.get(o.piperef).outQueue);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
        topPipelines.addAll(outputpipelines);
        outputpipelines = Collections.unmodifiableSet(outputpipelines);
        senders = Collections.unmodifiableList(senders);
        newProperties.put(Properties.PROPSNAMES.SENDERS.toString(), senders);

        newProperties.put(Properties.PROPSNAMES.TOPPIPELINE.toString(), Collections.unmodifiableSet(topPipelines));

        // Allows the system properties to override any properties given in the configuration file
        // But only if they are not some of the special internal properties
        Set<String> privatepropsnames = new HashSet<>(Properties.PROPSNAMES.values().length);
        Arrays.stream(Properties.PROPSNAMES.values()).forEach(i -> privatepropsnames.add(i.toString()));;
        System.getProperties().entrySet().stream().filter(i -> ! privatepropsnames.contains(i)).forEach(i -> newProperties.put(i.getKey().toString(), i.getValue()));
        return new Properties(newProperties);
    }

    private Pipeline parsePipeline(ConfigListener.PipenodesList desc, String currentPipeLineName, int depth, AtomicInteger subPipeCount) throws ConfigException {
        List<Processor> allSteps = new ArrayList<Processor>() {
            @Override
            public String toString() {
                StringBuilder buffer = new StringBuilder();
                buffer.append("PipeList(");
                for(Processor i: this) {
                    buffer.append(i);
                    buffer.append(", ");
                }
                buffer.setLength(buffer.length() - 2);
                buffer.append(')');
                return buffer.toString();
            }
        };

        desc.processors.stream().map(i -> {
            return getProcessor(i, currentPipeLineName, depth, subPipeCount);
        }).forEach(allSteps::add);
        Pipeline pipe = new Pipeline(allSteps, currentPipeLineName + (depth == 0 ? "" : "$" + subPipeCount.getAndIncrement()), desc.nextPipelineName);
        return pipe;
    }

    private Processor getProcessor(ConfigListener.Pipenode i, String currentPipeLineName, int depth, AtomicInteger subPipeLine) throws ConfigException {
        Processor t;
        if (i instanceof ConfigListener.PipeRef){
            ConfigListener.PipeRef cpr = (ConfigListener.PipeRef) i;
            NamedSubPipeline pr = new NamedSubPipeline();
            pr.setPipeRef(cpr.pipename);
            t = pr;
        } else if (i instanceof ConfigListener.PipenodesList){
            subPipeLine.incrementAndGet();
            AnonymousSubPipeline subpipe = new AnonymousSubPipeline();
            Pipeline p = parsePipeline((ConfigListener.PipenodesList)i, currentPipeLineName, depth + 1, subPipeLine);
            subpipe.setPipeline(p);
            t = subpipe;
        } else if (i instanceof ConfigListener.ProcessorInstance) {
            ConfigListener.ProcessorInstance ti = (ConfigListener.ProcessorInstance) i;

            t = (Processor) parseObjectDescription(ti, emptyConstructor, currentPipeLineName, depth, subPipeLine);
        } else {
            throw new RuntimeException("Unreachable code for " + i);
        }
        return t;
    }

    private <T, C> T parseObjectDescription(ConfigListener.ObjectDescription desc, ThrowingFunction<Class<T>, T> constructor) throws ConfigException {
        return parseObjectDescription(desc, constructor, null, 0, null);
    }

    private <T, C> T parseObjectDescription(ConfigListener.ObjectDescription desc, ThrowingFunction<Class<T>, T> constructor, String currentPipeLineName, int depth, AtomicInteger numSubpipe) throws ConfigException {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) classLoader.loadClass(desc.clazz);
            T object = constructor.apply(clazz);

            for(Entry<String, ObjectReference> i: desc.beans.entrySet()) {
                ObjectReference ref = i.getValue();
                Object beanValue;
                if(ref instanceof ConfigListener.ObjectWrapped) {
                    beanValue = ((ConfigListener.ObjectWrapped) ref).wrapped;
                    if (beanValue instanceof ConfigListener.Pipenode) {
                        beanValue = getProcessor((ConfigListener.Pipenode) beanValue, currentPipeLineName, depth, numSubpipe);
                    }
                } else if (ref instanceof ConfigListener.ObjectDescription) {
                    beanValue = parseObjectDescription((ConfigListener.ObjectDescription) ref, emptyConstructor, currentPipeLineName, depth + 1, numSubpipe);
                } else if (ref == null){
                    beanValue = null;
                } else {
                    throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz, ref.getClass().getCanonicalName()), desc.stream.getSourceName(), desc.ctx.start);
                }
                try {
                    BeansManager.beanSetter(object, i.getKey(), beanValue);
                } catch (InvocationTargetException ex) {
                    throw new ConfigException(String.format("Invalid bean '%s.%s': %s", desc.clazz, i.getKey(), ex.getCause()), desc.stream.getSourceName(), desc.ctx.start, ex.getCause());
                }
            }
            return object;
        } catch (ClassNotFoundException e) {
            throw new ConfigException(String.format("Unknown class '%s'", desc.clazz), desc.stream.getSourceName(), desc.ctx.start);
        } catch (ConfigException e) {
            throw e;
        } catch (RuntimeException | ExceptionInInitializerError e) {
            Throwable rootCause = e;
            if(rootCause.getCause() instanceof InvocationTargetException) {
                rootCause = (InvocationTargetException) rootCause.getCause();
            }
            throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz, rootCause.getMessage()), desc.stream.getSourceName(), desc.ctx.start, rootCause);
        }
    }

    ClassLoader doClassLoader(Object[] pathElements) throws IOException {

        final Collection<URL> urls = new ArrayList<URL>();

        // Needed for all the lambda that throws exception
        ThrowingPredicate<Path> filterReadable = i -> ! Files.isHidden(i);
        ThrowingConsumer<Path> toUrl = i -> urls.add(i.toUri().toURL());
        Arrays.stream(pathElements)
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

        return new URLClassLoader(urls.toArray(new URL[] {})) {
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
