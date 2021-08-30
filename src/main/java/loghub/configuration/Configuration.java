package loghub.configuration;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import loghub.Event;
import loghub.Helpers.ThrowingConsumer;
import loghub.Helpers.ThrowingPredicate;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.RouteParser.ArrayContext;
import loghub.RouteParser.BeanValueContext;
import loghub.RouteParser.ConfigurationContext;
import loghub.RouteParser.LiteralContext;
import loghub.RouteParser.PropertyContext;
import loghub.RouteParser.SourcedefContext;
import loghub.RouteParser.SourcesContext;
import loghub.Source;
import loghub.configuration.ConfigListener.Input;
import loghub.configuration.ConfigListener.Output;
import loghub.receivers.Receiver;
import loghub.senders.Sender;

public class Configuration {

    private static final Logger logger = LogManager.getLogger();

    private static final int DEFAULTQUEUEDEPTH = 100;
    private static final int DEFAULTQUEUEWEIGHT = 4;

    private List<Receiver> receivers;
    private Set<String> inputpipelines = new HashSet<>();
    private Set<String> outputpipelines = new HashSet<>();
    private Map<String, Source> sources = new HashMap<>();
    private List<Sender> senders;
    private ClassLoader classLoader = Configuration.class.getClassLoader();
    private SecretsHandler secrets = null;

    Configuration() {
    }

    public static Properties parse(String fileName) throws IOException, ConfigException {
        Configuration conf = new Configuration();
        return conf.runparsing(CharStreams.fromFileName(fileName));
    }

    public static Properties parse(InputStream is) throws IOException, ConfigException {
        Configuration conf = new Configuration();
        return conf.runparsing(CharStreams.fromStream(is));
    }

    public static Properties parse(Reader r) throws ConfigException, IOException {
        Configuration conf = new Configuration();
        return conf.runparsing(CharStreams.fromReader(r));
    }

    private Properties runparsing(CharStream cs) throws ConfigException {
        try {
            Map<String, PropertyContext> propertiesContext = new HashMap<>();
            ConfigListener conflistener = new ConfigListener();
            ParseTreeWalker walker = new ParseTreeWalker();

            RouteParser.ConfigurationContext tree = getTree(cs, conflistener);
            Set<String> lockedProperties = resolveProperties(tree, conflistener, propertiesContext);
            conflistener.classLoader = classLoader;
            conflistener.secrets = secrets;

            resolveSources(tree, conflistener);
            walker.walk(conflistener, tree);

            Consumer<Path> parseSubFile = filePath -> {
                try {
                    if (Files.isHidden(filePath)) {
                        return;
                    }
                    logger.debug("parsing {}", filePath);
                    CharStream localcs = CharStreams.fromPath(filePath);
                    RouteParser.ConfigurationContext localtree = getTree(localcs, conflistener);
                    lockedProperties.forEach( i -> conflistener.lockProperty(i));
                    resolveProperties(localtree, conflistener, propertiesContext);
                    walker.walk(conflistener, localtree);
                } catch (IOException e) {
                    throw new ConfigException(e.getMessage(), filePath.toString(), e);
                }
            };
            if (propertiesContext.containsKey("includes")) {
                Arrays.stream(getStringOrArrayLitteral(propertiesContext.remove("includes").beanValue()))
                .forEach( sourceName -> {
                    Path sourcePath = Paths.get(sourceName);
                    if (Files.isRegularFile(sourcePath)) {
                        // A file is given
                        parseSubFile.accept(sourcePath);
                    } else if (Files.isDirectory(sourcePath)) {
                        // A directory is given
                        try {
                            Files.list(sourcePath).forEach( i -> parseSubFile.accept(i));
                        } catch (IOException e) {
                            throw new ConfigException(e.getMessage(), sourceName, e);
                        }
                    } else {
                        // It might be a directory/pattern
                        Path patternPath = sourcePath.getFileName();
                        Path parent = sourcePath.getParent();
                        if (Files.isDirectory(parent)) {
                            PathMatcher pm = parent.getFileSystem().getPathMatcher("glob:" + patternPath.toString());
                            try {
                                Files.list(parent)
                                .filter( i -> pm.matches(i.getFileName()))
                                .forEach(parseSubFile::accept);
                            } catch (IOException e) {
                                throw new ConfigException(e.getMessage(), sourceName, e);
                            }
                        }
                    }
                });
            }
            return analyze(conflistener);
        } catch (RecognitionException e) {
            if (e.getCtx() instanceof ParserRuleContext) {
                ParserRuleContext ctx = (ParserRuleContext) e.getCtx();
                throw new ConfigException(e.getMessage(), e.getInputStream().getSourceName(), ctx.start, e);
            } else {
                throw e;
            }
        }
    }

    private void resolveSources(ConfigurationContext tree, ConfigListener conflistener) throws ConfigException {
        for (SourcesContext sc: tree.sources()) {
            for (SourcedefContext sdc: sc.sourcedef()) {
                String name = sdc.identifier().getText();
                if (! conflistener.sources.containsKey("name")) {
                    String className = sdc.object().QualifiedIdentifier().getText();
                    // Pre-create source object
                    ConfigListener.SourceProvider sp = new ConfigListener.SourceProvider();
                    sp.source = (Source) conflistener.getObject(className, sdc).wrapped;
                    conflistener.sources.put(name, sp);
                } else {
                    throw new RecognitionException("Source redifined", conflistener.parser, conflistener.stream, sdc);
                }
            }
        }
    }

    private Set<String> resolveProperties(RouteParser.ConfigurationContext tree, ConfigListener conflistener, Map<String, PropertyContext> propertiesContext) throws ConfigException {

        Set<String> lockedProperties = new HashSet<>();

        tree.property().forEach( pc -> propertiesContext.put(pc.propertyName().getText(), pc) );

        if (propertiesContext.containsKey("locale")) {
            String localString = getStringLitteral(propertiesContext.remove("locale").beanValue());
            if (localString != null) {
                logger.debug("setting locale to {}", localString);
                Locale l = Locale.forLanguageTag(localString);
                Locale.setDefault(l);
                lockedProperties.add("locale");
            }
        }

        if (propertiesContext.containsKey("timezone")) {
            String tz = getStringLitteral(propertiesContext.remove("timezone").beanValue());
            if (tz != null) {
                try {
                    logger.debug("setting time zone to {}", tz);
                    ZoneId id = ZoneId.of(tz);
                    TimeZone.setDefault(TimeZone.getTimeZone(id));
                } catch (DateTimeException e) {
                    logger.error("Invalid timezone {}: {}", tz, e.getMessage());
                }
                lockedProperties.add("timezone");
            }
        }

        // resolve the plugins property to define the class loader
        if (propertiesContext.containsKey("plugins")) {
            PropertyContext pc = propertiesContext.remove("plugins");
            String[] path = getStringOrArrayLitteral(pc.beanValue());
            if(path.length > 0) {
                try {
                    logger.debug("Looking for plugins in {}", (Object[])path);
                    classLoader = doClassLoader(path);
                } catch (IOException ex) {
                    throw new ConfigException("can't load plugins: " + ex.getMessage(), conflistener.stream.getSourceName(), pc.start, ex);
                } 
            }
        }
        lockedProperties.add("plugins");

        // resolve some top level log4j properties
        URI log4JUri = null;
        PropertyContext pc = null;
        if (propertiesContext.containsKey("log4j.configURL")) {
            pc = propertiesContext.remove("log4j.configURL");
            try {
                log4JUri = getLitteral(pc.beanValue(), i -> i.stringLiteral().getText(), j -> {
                    try {
                        return new URL(j).toURI();
                    } catch (MalformedURLException | URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                });
                logger.debug("Configured log4j URL to {}", log4JUri);
            } catch (RuntimeException e) {
                logger.error("Invalid log4j URL");
            }
        } else if (propertiesContext.containsKey("log4j.configFile")) {
            pc = propertiesContext.remove("log4j.configFile");
            log4JUri = getLitteral(pc.beanValue(), i -> i.stringLiteral().getText(), j -> {
                return new File(j).toURI();
            });
            logger.debug("Configured log4j URL to {}", log4JUri);
        }
        if (log4JUri != null) {
            // Try to read the log4j configuration, because log4j2 intercept possible exceptions and don't forward them
            try(InputStream is = log4JUri.toURL().openStream()) {
                int toread  = 0;
                while ((toread = is.available()) != 0) {
                    is.skip(toread);
                }
            } catch (IOException e) {
                throw new ConfigException(e.getMessage(), conflistener.stream.getSourceName(), pc.start, e);
            }
            LoggerContext ctx = (LoggerContext) LogManager.getContext(classLoader, true);
            // Possible exception are already catched (I hope)
            ctx.setConfigLocation(log4JUri);
            logger.debug("log4j reconfigured");
        }
        lockedProperties.add("log4j.configURL");
        lockedProperties.add("log4j.configFile");
        
        if (propertiesContext.containsKey("secrets.source")) {
            try {
                String secretsSource = getStringLitteral(propertiesContext.remove("secrets.source").beanValue());
                secrets = SecretsHandler.load(secretsSource);
                lockedProperties.add("secrets.source");
            } catch (IOException ex) {
                throw new ConfigException("can't load secret store: " + ex.getMessage(), conflistener.stream.getSourceName(), pc.start, ex);
            }
        }

        return lockedProperties;
    }

    private String getStringLitteral(BeanValueContext beanValue) {
        return getLitteral(beanValue, i -> beanValue.literal().getText(), j -> j);
    }

    private String[] getStringOrArrayLitteral(BeanValueContext beanValue) {
        ArrayContext ac = beanValue.array();

        String contentString;
        if (ac != null) {
            return ac.beanValue().stream().map( i -> i.getText()).toArray(String[]::new);
        } else if ((contentString = getStringLitteral(beanValue)) != null) {
            return new String[] {contentString};
        } else {
            return new String[] {};
        }

    }

    private <T> T getLitteral(BeanValueContext beanValue, Function<LiteralContext, String> read, Function<String, T> convert) {
        if (beanValue != null) {
            LiteralContext lc = beanValue.literal();
            if (lc != null) {
                String content = read.apply(lc);
                if (content != null) {
                    return convert.apply(content);
                }
            }
        }
        return null;
    }

    private RouteParser.ConfigurationContext getTree(CharStream cs, ConfigListener conflistener) throws ConfigException {

        //Passing the input to the lexer to create tokens
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        //Passing the tokens to the parser to create the parse tree.
        RouteParser parser = new RouteParser(tokens);
        conflistener.parser = parser;
        conflistener.stream = cs;
        parser.removeErrorListeners();
        ConfigErrorListener errListener = new ConfigErrorListener();
        parser.addErrorListener(errListener);
        RouteParser.ConfigurationContext tree = parser.configuration();
        return tree;

    }

    private Properties analyze(ConfigListener conf) throws ConfigException {

        Map<String, Object> newProperties = new HashMap<String, Object>(conf.properties.size() + Properties.PROPSNAMES.values().length + System.getProperties().size());

        // Resolvers properties found and and it to new properties
        @SuppressWarnings("unchecked")
        Function<Object, Object> resolve = i -> {
            return ((i instanceof ConfigListener.ObjectWrapped) ? ((ConfigListener.ObjectWrapped<Object>) i).wrapped : i);
        };
        conf.properties.entrySet().stream().forEach(i-> newProperties.put(i.getKey(), resolve.apply(i.getValue())));

        Map<String, Pipeline> namedPipeLine = new HashMap<>(conf.pipelines.size());

        newProperties.put(Properties.PROPSNAMES.CLASSLOADERNAME.toString(), classLoader);

        Set<Pipeline> pipelines = new HashSet<>();
        // Generate all the named pipeline
        conf.depth = 0;
        for(Entry<String, ConfigListener.PipenodesList> e: conf.pipelines.entrySet()) {
            String name = e.getKey(); 
            Pipeline p = conf.parsePipeline(e.getValue(), name);
            pipelines.add(p);
            namedPipeLine.put(name, p);
        }
        newProperties.put(Properties.PROPSNAMES.PIPELINES.toString(), Collections.unmodifiableSet(pipelines));
        namedPipeLine = Collections.unmodifiableMap(namedPipeLine);
        newProperties.put(Properties.PROPSNAMES.NAMEDPIPELINES.toString(), namedPipeLine);

        //Find the queue depth
        int queuesDepth = newProperties.containsKey("queueDepth") ? (Integer) newProperties.remove("queueDepth") : DEFAULTQUEUEDEPTH;
        newProperties.put(Properties.PROPSNAMES.QUEUESDEPTH.toString(), queuesDepth);
        
        // Find the queue weight
        int queueWeight = newProperties.containsKey("queueWeigth") ? (Integer) newProperties.remove("queueWeigth") : DEFAULTQUEUEWEIGHT;

        PriorityBlockingQueue mainQueue = new PriorityBlockingQueue(queuesDepth, queueWeight);
        Map<String, BlockingQueue<Event>> outputQueues = new HashMap<>(namedPipeLine.size());
        conf.outputPipelines.forEach( i-> outputQueues.put(i, new LinkedBlockingQueue<Event>(queuesDepth)));

        newProperties.put(Properties.PROPSNAMES.FORMATTERS.toString(), conf.formatters);
        newProperties.put(Properties.PROPSNAMES.MAINQUEUE.toString(), mainQueue);
        newProperties.put(Properties.PROPSNAMES.OUTPUTQUEUE.toString(), outputQueues);

        // Fill the receivers list
        receivers = new ArrayList<>();
        for(Input i: conf.inputs) {
            if(i.piperef == null || ! namedPipeLine.containsKey(i.piperef)) {
                throw new ConfigException("Invalid input, no destination pipeline: " + i);
            }
            for(ConfigListener.ObjectWrapped<Receiver> desc: i.receiver) {
                Pipeline p = namedPipeLine.get(i.piperef);
                Receiver r = desc.wrapped;
                r.setOutQueue(mainQueue);
                r.setPipeline(p);
                receivers.add(r);
            }
            inputpipelines.add(i.piperef);
        }
        inputpipelines = Collections.unmodifiableSet(inputpipelines);
        receivers = Collections.unmodifiableList(receivers);
        newProperties.put(Properties.PROPSNAMES.RECEIVERS.toString(), receivers);

        // Fill the senders list
        senders = new ArrayList<>();
        for (Output o: conf.outputs) {
            if (o.piperef == null || ! namedPipeLine.containsKey(o.piperef)) {
                throw new IllegalArgumentException("Invalid output, no source pipeline: " + o);
            }
            for (ConfigListener.ObjectWrapped<Sender> desc: o.sender) {
                BlockingQueue<Event> out = outputQueues.get(o.piperef);
                Sender s = desc.wrapped;
                s.setInQueue(out);
                //logger.debug("sender {} source point will be {}", () -> s, () -> namedPipeLine.get(o.piperef).outQueue);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
        outputpipelines = Collections.unmodifiableSet(outputpipelines);
        senders = Collections.unmodifiableList(senders);
        newProperties.put(Properties.PROPSNAMES.SENDERS.toString(), senders);

        sources = conf.sources.entrySet().stream()
                        .map( e -> new AbstractMap.SimpleEntry<String, Source>(e.getKey(), e.getValue().source))
                        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()))
                        ;
        newProperties.put(Properties.PROPSNAMES.SOURCES.toString(), Collections.unmodifiableMap(sources));

        // Allows the system properties to override any properties given in the configuration file
        // But only if they are not some of the special internal properties
        Set<String> privatepropsnames = new HashSet<>(Properties.PROPSNAMES.values().length);
        Arrays.stream(Properties.PROPSNAMES.values()).forEach(i -> privatepropsnames.add(i.toString()));;
        System.getProperties().entrySet().stream().filter(i -> ! privatepropsnames.contains(i.getKey())).forEach(i -> newProperties.put(i.getKey().toString(), i.getValue()));
        return new Properties(newProperties);
    }

    private static final class LogHubClassloader extends URLClassLoader {
        public LogHubClassloader(URL[] urls) {
            super(urls);
        }
        @Override
        public String toString() {
            return "Loghub's class loader";
        }
    };

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
                    throw new RuntimeException(e);
                }
            }
        });

        //Add myself to class loader, so the custom class loader is used in priority
        Path myself = locateResourcefile("loghub");
        if(myself.endsWith("loghub")) {
            myself = myself.getParent();
        }
        urls.add(myself.toUri().toURL());

        return new LogHubClassloader(urls.toArray(new URL[] {}));
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

}
