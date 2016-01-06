package loghub.configuration;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
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
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.PipeStep;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.Sender;
import loghub.Processor;
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

        if(conf.properties.containsKey("plugins") && ! conf.properties.get("plugins").toString().isEmpty()) {
            try {
                classLoader = doClassLoader(conf.properties.get("plugins").toString());
            } catch (IOException ex) {
                throw new RuntimeException("can't load plugins: " + ex.getMessage(), ex);
            }
        }

        // Generate all the named pipeline
        namedPipeLine = new HashMap<>(conf.pipelines.size());
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
                Receiver r = (Receiver) parseObjectDescription(desc);
                logger.debug("receiver {} destination point will be {}", () -> i, () -> namedPipeLine.get(i.piperef).inEndpoint);
                r.setEndpoint(namedPipeLine.get(i.piperef).inEndpoint);
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
                Sender s = (Sender) parseObjectDescription(desc);
                logger.debug("sender {} source point will be {}", () -> s, () -> namedPipeLine.get(o.piperef).outEndpoint);
                s.setEndpoint(namedPipeLine.get(o.piperef).outEndpoint);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
        outputpipelines = Collections.unmodifiableSet(outputpipelines);
        senders = Collections.unmodifiableList(senders);

        joins = Collections.unmodifiableSet(conf.joins);
        
        conf.properties.put(Properties.CLASSLOADERNAME, classLoader);
        properties = new Properties(conf.properties);
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
        this.pipelines.add(pipe);
        return pipe;
    }

    private Processor getProcessor(ConfigListener.Processor i, String currentPipeLineName, List<Pipeline> currentPipeList, int depth) {
        Processor t;
        if(i instanceof ConfigListener.ProcessorInstance) {
            ConfigListener.ProcessorInstance ti = (ConfigListener.ProcessorInstance) i;
            t = (Processor) parseObjectDescription(ti);
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

    private <T> T parseObjectDescription(ConfigListener.ObjectDescription desc) {
        try {
            @SuppressWarnings("unchecked")
            Class<T> clazz = (Class<T>) classLoader.loadClass(desc.clazz);
            T object = clazz.getConstructor().newInstance();
            for(Entry<String, ObjectReference> i: desc.beans.entrySet()) {
                ObjectReference ref = i.getValue();
                Object beanValue;
                if(i.getValue() instanceof ConfigListener.ObjectWrapped) {
                    beanValue = ((ConfigListener.ObjectWrapped) ref).wrapped;
                } else if (i.getValue() instanceof ConfigListener.ObjectDescription) {
                    beanValue = parseObjectDescription((ConfigListener.ObjectDescription) ref);
                } else {
                    throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz), desc.ctx.start, desc.ctx.stop);
                }
                BeansManager.beanSetter(object, i.getKey(), beanValue);
            }
            return object;
        } catch (ClassNotFoundException e) {
            logger.debug(desc.clazz);
            logger.debug(desc.ctx);
            throw new ConfigException(String.format("Unknown class '%s'", desc.clazz), desc.ctx.start, desc.ctx.stop);
        } catch (InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException | ExceptionInInitializerError e) {
            throw new ConfigException(String.format("Invalid class '%s': %s", desc.clazz, e), desc.ctx.start, desc.ctx.stop);
        }
    }

    @FunctionalInterface
    public interface ThrowingPredicate<T> extends Predicate<T> {

        @Override
        default boolean test(final T elem) {
            try {
                return testThrows(elem);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        boolean testThrows(T elem) throws Exception;

    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> extends Consumer<T> {

        @Override
        default void accept(final T elem) {
            try {
                acceptThrows(elem);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        void acceptThrows(T elem) throws Exception;

    }

    ClassLoader doClassLoader(String extendedclasspath) throws IOException {

        final Collection<URL> urls = new ArrayList<URL>();

        // Needed for all the lambda that throws exception
        ThrowingPredicate<Path> filterReadable = i -> ! Files.isHidden(i);
        ThrowingConsumer<Path> toUrl = i -> urls.add(i.toUri().toURL());

        Path[] components;
        components = (Path[]) Arrays.stream(extendedclasspath.split(File.pathSeparator))
                .map((i) -> Paths.get(i))
                .filter(i -> Files.isReadable(i))
                .filter(filterReadable)
                .filter(i -> (Files.isRegularFile(i) && i.toString().endsWith(".jar")) || Files.isDirectory(i))
                .toArray(Path[]::new);
        for(Path i: components) {
            toUrl.accept(i);
            if(Files.isDirectory(i)) {
                Files.list(i)
                .filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".jar"))
                .forEach(toUrl);
            }
        }

        return new URLClassLoader(urls.toArray(new URL[] {}), getClass().getClassLoader()) {
            @Override
            public String toString() {
                return "Loghub's class loader";
            }
        };
    }

    public Collection<Receiver> getReceivers() {
        return Collections.unmodifiableList(receivers);
    }

    public Collection<Sender> getSenders() {
        return Collections.unmodifiableList(senders);
    }

}
