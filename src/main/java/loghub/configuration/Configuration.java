package loghub.configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuleContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import groovy.lang.GroovyClassLoader;
import loghub.Helpers;
import loghub.Helpers.ThrowingConsumer;
import loghub.Helpers.ThrowingPredicate;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.RouteLexer;
import loghub.RouteParser;
import loghub.RouteParser.ArrayContext;
import loghub.RouteParser.PropertyContext;
import loghub.RouteParser.SourcedefContext;
import loghub.RouteParser.SourcesContext;
import loghub.configuration.ConfigListener.Input;
import loghub.configuration.ConfigListener.Output;
import loghub.events.Event;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.security.ssl.ContextLoader;
import loghub.senders.Sender;
import loghub.sources.Source;

public class Configuration {

    private static final Logger logger = LogManager.getLogger();

    private static final int DEFAULTQUEUEDEPTH = 100;
    private static final int DEFAULTQUEUEWEIGHT = 4;

    private Set<String> inputpipelines = new HashSet<>();
    private Set<String> outputpipelines = new HashSet<>();
    private ClassLoader classLoader = Configuration.class.getClassLoader();
    private GroovyClassLoader groovyClassLoader = new GroovyClassLoader(classLoader);
    private CacheManager cacheManager = new CacheManager(classLoader);
    private SecretsHandler secrets = null;
    private final Map<String, String> lockedProperties = new HashMap<>();
    private final Map<String, Object> configurationProperties = new HashMap<>();
    private final Set<Path> loadedConfigurationFiles = new HashSet<>();
    private final ConfigErrorListener errListener = new ConfigErrorListener();

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

    @lombok.Builder
    private static class Tree {
        private final CharStream stream;
        private final RouteParser.ConfigurationContext config;
        private final RouteParser parser;
        public static Tree of(CharStream stream, RouteParser.ConfigurationContext config, RouteParser parser) {
            return Tree.builder().stream(stream).config(config).parser(parser).build();
        }
    }

    private boolean findStreams(CharStream cs, List<Tree> trees) {
        //Passing the input to the lexer to create tokens
        RouteLexer lexer = new RouteLexer(cs);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        //Passing the tokens to the parser to create the parse tree.
        RouteParser parser = new RouteParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(errListener);
        RouteParser.ConfigurationContext configurationContext = parser.configuration();
        // First pass, to identify the class loader to use
        // The class loader must be defined in the first configuration file, not included one
        if (! lockedProperties.containsKey("plugins")) {
            logger.debug("Resolving classpath");
            resolveClassPath(configurationContext, cs);
        }
        parser.filter.setClassLoader(classLoader);
        logger.debug("Find configuration root");
        Tree tree = Tree.of(cs, configurationContext, parser);
        logger.debug("Scan properties");
        scanProperty(tree);
        trees.add(tree);
        return tree.config.property().stream()
          .filter(p -> "includes".equals(p.propertyName.getText()))
          .map(pc -> Arrays.stream(getStringOrArrayLiteral(pc.beanValue()))
                           .map(Paths::get)
                           .map(p -> consumeIncludes(p, trees))
                           .reduce(Boolean.FALSE, (a, b) -> a || b))
         .reduce((a, b) -> a || b).orElse(true);
    }

    private void resolveClassPath(RouteParser.ConfigurationContext configurationContext, CharStream cs) {
        for (PropertyContext pc: configurationContext.property()) {
            if ("plugins".equals(pc.propertyName.getText())) {
                String[] path = getStringOrArrayLiteral(pc.beanValue());
                if (path.length > 0) {
                    try {
                        logger.debug("Looking for plugins in {}", (Object[])path);
                        classLoader = doClassLoader(path);
                        groovyClassLoader = new GroovyClassLoader(classLoader);
                        cacheManager = new CacheManager(classLoader);
                        lockedProperties.put("plugins", pc.beanValue().getText());
                    } catch (IOException | UncheckedIOException ex) {
                        throw new ConfigException("can't load plugins: " + ex.getMessage(), cs.getSourceName(), pc.start, ex);
                    }
                }
                break;
            }
        }
    }

    private CharStream pathToCharStream(Path sourcePath) {
        try {
            logger.debug("Loading a configuration from {}", sourcePath);
            return CharStreams.fromPath(sourcePath);
        } catch (IOException ex) {
            throw new ConfigException(ex.getMessage(), sourcePath.toString(), ex);
        }
    }

    private boolean loadStream(Stream<Path> files, List<Tree> trees) {
        return files.filter(p -> {
                 Path realPath;
                 try {
                     realPath = p.toRealPath();
                     return (! Files.isHidden(p) && loadedConfigurationFiles.add(realPath));
                 } catch (IOException ex) {
                     throw new UncheckedIOException(ex);
                 }
             })
             .sorted(Helpers.NATURALSORTPATH)
             .map(this::pathToCharStream)
             .map(cs -> findStreams(cs, trees))
             .reduce(Boolean.FALSE, (a, b) -> a || b);
    }

    private boolean consumeIncludes(Path sourcePath, List<Tree> trees) {
        boolean found = false;
        if ( ! Files.exists(sourcePath)) {
            // It might be a glob pattern
            Path progress = sourcePath.isAbsolute() ? sourcePath.getRoot() : Paths.get(".").normalize();
            for (Path p: sourcePath) {
                Path trypath = progress.resolve(p);
                if (! Files.exists(trypath)) {
                    break;
                } else {
                    progress = trypath;
                }
            }
            PathMatcher pm = progress.getFileSystem().getPathMatcher("glob:" + sourcePath);
            try {
                found = loadStream(Files.find(progress, 1000, (p, a) -> pm.matches(p)), trees);
            } catch (IOException | UncheckedIOException e) {
                throw new ConfigException(e.getMessage(), sourcePath.toString(), e);
            }
        } else if (Files.isDirectory(sourcePath)) {
            // A directory is given
            try {
                found = loadStream(Files.list(sourcePath), trees);
            } catch (IOException | UncheckedIOException e) {
                throw new ConfigException(e.getMessage(), sourcePath.toString(), e);
            }
        } else {
            try {
                // Something to directly read is given
                if (loadedConfigurationFiles.add(sourcePath.toRealPath())) {
                    found = findStreams(pathToCharStream(sourcePath), trees);
                }
            } catch (IOException e) {
                throw new ConfigException(e.getMessage(), sourcePath.toString(), e);
            }
         }
        return found;
    }

    private void scanProperty(Tree tree) {
        Map<String, PropertyContext> currentProperties = new HashMap<>();
        for (PropertyContext pc: tree.config.property()) {
            currentProperties.put(pc.propertyName.getText(), pc);
        }
        // Don’t change order, it's meaningfully
        // locale and timezone first, to check for output format
        // everything else must be set latter
        Optional.ofNullable(currentProperties.remove("locale")).ifPresent(this::processLocaleProperty);
        Optional.ofNullable(currentProperties.remove("timezone")).ifPresent(this::processTimezoneProperty);
        Optional.ofNullable(currentProperties.remove("log4j.configURL")).ifPresent(pc -> processLog4jUriProperty("log4j.configURL", pc, tree));
        Optional.ofNullable(currentProperties.remove("log4j.configFile")).ifPresent(pc -> processLog4jUriProperty("log4j.configFile", pc, tree));
        Optional.ofNullable(currentProperties.remove("secrets.source")).ifPresent(pc -> processSecretSource(pc, tree));

        // Iterator needed to remove entries while iterating
        for (Iterator<Entry<String, PropertyContext>> it = currentProperties.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<String, PropertyContext> e = it.next();
            if ("includes".equals(e.getKey())) {
                it.remove();
                continue;
            }
            try {
                configurationProperties.put(e.getKey(), resolveBean(e.getValue().beanValue()));
                it.remove();
            } catch (IllegalArgumentException ex) {
                assert false : String.format("%s: %s", e.getKey(), ex.getMessage());
            }
        }
        assert currentProperties.size() == 0;
    }

    private Object resolveBean(RouteParser.BeanValueContext bvc) {
        if (bvc.stringLiteral() != null) {
            return bvc.getText();
        } else if (bvc.booleanLiteral() != null) {
            return Boolean.valueOf(bvc.getText());
        } else if (bvc.characterLiteral() != null) {
            return bvc.getText().charAt(0);
        } else if (bvc.integerLiteral() != null) {
            return Integer.valueOf(bvc.getText());
        } else if (bvc.nullLiteral() != null) {
            return null;
        } else if (bvc.array() != null) {
            return bvc.array().arrayContent().beanValue().stream().map(this::resolveBean).toArray(Object[]::new);
        } else if (bvc.secret() != null) {
            return bvc.secret();
        } else {
            throw new IllegalArgumentException("bean value unidentified " + bvc.getText());
        }
    }

    private void processSecretSource(PropertyContext pc, Tree tree) {
        try {
            String secretsSource = pc.beanValue().getText();
            secrets = SecretsHandler.load(secretsSource);
            logger.debug("Loaded secrets source {}", secretsSource);
            lockedProperties.put("secrets.source", secretsSource);
        } catch (IOException ex) {
            throw new ConfigException("can't load secret store: " + ex.getMessage(), tree.stream.getSourceName(), pc.start, ex);
        }
    }

    private void processLog4jUriProperty(String propertyName, PropertyContext pc, Tree tree) {
        URI log4JUri =  Helpers.fileUri(pc.beanValue().getText());
        resolverLogger(log4JUri, pc, tree);
        logger.debug("Configured log4j URL to {}", log4JUri);
        lockedProperties.put(propertyName, pc.beanValue().getText());
    }

    private void resolverLogger(URI log4JUri, PropertyContext pc, Tree tree) {
        // Try to read the log4j configuration, because log4j2 intercept possible exceptions and don't forward them
        try (InputStream ignored = log4JUri.toURL().openStream()) {
            log4JUri.toURL().getContent();
        } catch (IOException e) {
            throw new ConfigException(e.getMessage(), tree.stream.getSourceName(), pc.start, e);
        }
        LoggerContext ctx = (LoggerContext) LogManager.getContext(classLoader, true);
        // Possible exception are already catched (I hope)
        ctx.setConfigLocation(log4JUri);
        logger.debug("log4j reconfigured");
    }

    private void processTimezoneProperty(PropertyContext pc) {
        String tz = pc.beanValue().getText();
        if (tz != null) {
            try {
                logger.debug("setting time zone to {}", tz);
                ZoneId id = ZoneId.of(tz);
                TimeZone.setDefault(TimeZone.getTimeZone(id));
            } catch (DateTimeException e) {
                logger.error("Invalid timezone {}: {}", tz, e.getMessage());
            }
        }
        lockedProperties.put("timezone", tz);
    }

    private void processLocaleProperty(PropertyContext pc) {
        String localString = pc.beanValue().getText();
        if (localString != null) {
            logger.debug("setting locale to {}", localString);
            Locale l = Locale.forLanguageTag(localString);
            Locale.setDefault(l);
        }
        lockedProperties.put("locale", localString);
    }

    private Properties runparsing(CharStream cs) throws ConfigException {
        try {
            List<Tree> trees = new ArrayList<>();

            boolean found = findStreams(cs, trees);
            if (! found) {
                throw new ConfigException("No Configuration files found");
            }
            Map<String, Object> resolvedSecrets = configurationProperties.entrySet().stream()
                                              .filter(e -> e.getValue() instanceof RouteParser.SecretContext)
                                              .map(this::resoveSecret)
                                              .collect(Collectors.toMap(Entry::getKey, Entry::getValue))
                                              ;

            configurationProperties.putAll(resolvedSecrets);
            configurationProperties.entrySet().removeIf(e -> e.getValue() == null);
            ConfigListener conflistener = ConfigListener.builder()
                                                        .classLoader(classLoader)
                                                        .groovyClassLoader(groovyClassLoader)
                                                        .secrets(secrets)
                                                        .lockedProperties(lockedProperties)
                                                        .sslContext(resolveSslContext())
                                                        .jaasConfig(resolveJaasConfig())
                                                        .jwtHandler(resolveJwtHander())
                                                        .cacheManager(cacheManager)
                                                        .build();
            logger.debug("Walk configuration");
            trees.forEach(t -> {
                resolveSources(t, conflistener);
                conflistener.startWalk(t.config, t.stream, t.parser);
            });
            return analyze(conflistener);
        } catch (RecognitionException e) {
            if (e.getCtx() instanceof ParserRuleContext) {
                ParserRuleContext ctx = (ParserRuleContext) e.getCtx();
                logger.error("File {}, line {}@{}: {}", () -> ctx.start.getInputStream().getSourceName(), () -> ctx.start.getLine(), () -> ctx.start.getCharPositionInLine(), () -> e.getMessage());
                throw new ConfigException(e.getMessage(), e.getInputStream().getSourceName(), ctx.start, e);
            } else {
                throw e;
            }
        }
    }

    private Map.Entry<String, Object> resoveSecret(Map.Entry<String, Object> e) {
        RouteParser.SecretContext ctx = (RouteParser.SecretContext) e.getValue();
        byte[] secret = secrets.get(ctx.id.getText());
        Object value;
        if (secret == null) {
            value = null;
        } else if (ctx.SecretAttribute() == null || ! "blob".equals(ctx.SecretAttribute().getText())) {
            value = new String(secret, StandardCharsets.UTF_8);
        } else {
            value = secret;
        }
        return new AbstractMap.SimpleEntry<>(e.getKey(), value);
    }

    private JWTHandler resolveJwtHander() {
        Map<String, Object> properties = Helpers.filterPrefix(configurationProperties, "jwt");
        configurationProperties.entrySet().removeIf(i -> i.getKey().startsWith("jwt."));
        return JWTHandler.getBuilder()
                         .secret(Optional.ofNullable(properties.get("secret")).map(Object::toString).orElse(null))
                         .setAlg(Optional.ofNullable(properties.get("alg")).map(Object::toString).orElse(null))
                         .build();
    }

    private void resolveSources(Tree tree, ConfigListener conflistener) throws ConfigException {
        for (SourcesContext sc: tree.config.sources()) {
            for (SourcedefContext sdc: sc.sourcedef()) {
                String name = sdc.identifier().getText();
                if (! conflistener.sources.containsKey("name")) {
                    conflistener.sources.put(name, new AtomicReference<>());
                } else {
                    throw new RecognitionException("Source redefined", tree.parser, tree.stream, sdc);
                }
            }
        }
    }

    private SSLContext resolveSslContext() {
        SSLContext ssl;
        Map<String, Object> sslprops = Helpers.filterPrefix(configurationProperties, "ssl");
        configurationProperties.entrySet().removeIf(i -> i.getKey().startsWith("ssl."));
        if (! sslprops.isEmpty()) {
            ssl = ContextLoader.build(classLoader, sslprops);
        } else {
            try {
                ssl = SSLContext.getDefault();
            } catch (NoSuchAlgorithmException ex) {
                throw new ConfigException("SSLContext failed to configure: " + ex);
            }
        }
        if (ssl == null) {
            throw new ConfigException("SSLContext failed to configure");
        } else {
            return ssl;
        }
    }

    private javax.security.auth.login.Configuration resolveJaasConfig() {
        if (configurationProperties.containsKey("jaasConfig")) {
            String jaasConfigFilePath = (String) configurationProperties.remove("jaasConfig");
            URIParameter cp = new URIParameter(Helpers.fileUri(jaasConfigFilePath));
            try {
                return javax.security.auth.login.Configuration.getInstance("JavaLoginConfig", cp);
            } catch (NoSuchAlgorithmException e) {
                throw new ConfigException("JavaLoginConfig unavailable", e);
            }
        } else {
            return null;
        }
    }

    private String[] getStringOrArrayLiteral(RouteParser.BeanValueContext beanValue) {
        ArrayContext ac = beanValue.array();
        String contentString;
        if (ac != null) {
            return ac.arrayContent().beanValue().stream().map(RuleContext::getText).toArray(String[]::new);
        } else if ((contentString = beanValue.stringLiteral().getText()) != null) {
            return new String[] {contentString};
        } else {
            return new String[] {};
        }
    }

    private Properties analyze(ConfigListener conf) throws ConfigException {
        Map<String, Object> newProperties = new HashMap<>(conf.properties.size() + Properties.PROPSNAMES.values().length + System.getProperties().size());

        // Resolvers properties found and add it to new properties
        @SuppressWarnings("unchecked")
        UnaryOperator<Object> resolve = i -> ((i instanceof ConfigListener.ObjectWrapped) ? ((ConfigListener.ObjectWrapped<Object>) i).wrapped : i);
        conf.properties.forEach((key, value) -> newProperties.put(key, resolve.apply(value)));

        Map<String, Pipeline> namedPipeLine = new HashMap<>(conf.pipelines.size());

        newProperties.put(Properties.PROPSNAMES.CLASSLOADERNAME.toString(), classLoader);
        newProperties.put(Properties.PROPSNAMES.CACHEMANGER.toString(), cacheManager);
        newProperties.put(Properties.PROPSNAMES.SSLCONTEXT.toString(), conf.sslContext);
        newProperties.put(Properties.PROPSNAMES.JAASCONFIG.toString(), conf.jaasConfig);

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
        int queueWeight = newProperties.containsKey("queueWeight") ? (Integer) newProperties.remove("queueWeight") :
                                  (newProperties.containsKey("queueWeigth") ? (Integer) newProperties.remove("queueWeigth") : DEFAULTQUEUEWEIGHT);

        PriorityBlockingQueue mainQueue = new PriorityBlockingQueue(queuesDepth, queueWeight);
        Map<String, BlockingQueue<Event>> outputQueues = new HashMap<>(namedPipeLine.size());
        conf.outputPipelines.forEach( i-> outputQueues.put(i, new LinkedBlockingQueue<>(queuesDepth)));

        newProperties.put(Properties.PROPSNAMES.FORMATTERS.toString(), conf.formatters);
        newProperties.put(Properties.PROPSNAMES.MAINQUEUE.toString(), mainQueue);
        newProperties.put(Properties.PROPSNAMES.OUTPUTQUEUE.toString(), outputQueues);

        // Fill the receivers list
        List<Receiver<?, ?>> receivers = new ArrayList<>();
        for (Input i: conf.inputs) {
            if (i.piperef == null || ! namedPipeLine.containsKey(i.piperef)) {
                throw new ConfigException("Invalid input, no destination pipeline: " + i);
            }
            for (ConfigListener.ObjectWrapped<Receiver> desc: i.receiver) {
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
        List<Sender> senders = new ArrayList<>();
        for (Output o: conf.outputs) {
            if (o.piperef == null || ! namedPipeLine.containsKey(o.piperef)) {
                throw new IllegalArgumentException("Invalid output, no source pipeline: " + o);
            }
            for (ConfigListener.ObjectWrapped<Sender> desc: o.sender) {
                BlockingQueue<Event> out = outputQueues.get(o.piperef);
                Sender s = desc.wrapped;
                s.setInQueue(out);
                senders.add(s);
            }
            outputpipelines.add(o.piperef);
        }
        outputpipelines = Collections.unmodifiableSet(outputpipelines);
        senders = Collections.unmodifiableList(senders);
        newProperties.put(Properties.PROPSNAMES.SENDERS.toString(), senders);

        Map<String, Source> sources = conf.sources.entrySet().stream()
                                              .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue().get()))
                                              .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
                                                      AbstractMap.SimpleEntry::getValue));
        newProperties.put(Properties.PROPSNAMES.SOURCES.toString(), Collections.unmodifiableMap(sources));

        // Allows the system properties to override any properties given in the configuration file
        // But only if they are not some of the special internal properties
        Set<String> privatepropsnames = new HashSet<>(Properties.PROPSNAMES.values().length);
        Arrays.stream(Properties.PROPSNAMES.values()).forEach(i -> privatepropsnames.add(i.toString()));
        System.getProperties().entrySet().stream().filter(i -> ! privatepropsnames.contains(i.getKey())).forEach(i -> newProperties.put(i.getKey().toString(), i.getValue()));
        return new Properties(newProperties);
    }

    private static final class LogHubClassloader extends URLClassLoader {
        public LogHubClassloader(URL[] urls) {
            super(urls, LogHubClassloader.class.getClassLoader());
        }
        @Override
        public String toString() {
            return "Loghub's class loader";
        }
    }

    ClassLoader doClassLoader(Object[] pathElements) throws IOException {
        final Collection<URL> urls = new ArrayList<>();

        // Needed for all the lambda that throws exception
        ThrowingPredicate<Path> filterReadable = i -> ! Files.isHidden(i);
        ThrowingConsumer<Path> toUrl = i -> urls.add(i.toUri().toURL());
        Arrays.stream(pathElements)
        .map(i -> Paths.get(i.toString()))
        .filter(Files::isReadable)
        .filter(filterReadable)
        .filter(i -> (Files.isRegularFile(i) && i.toString().endsWith(".jar")) || Files.isDirectory(i))
        .forEach( i-> {
            toUrl.accept(i);
            if (Files.isDirectory(i)) {
                try (Stream<Path> files = Files.list(i)){
                    files.filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".jar"))
                         .forEach(toUrl);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
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
            @SuppressWarnings("ConstantConditions")
            String protocol = url.getProtocol();
            String file;
            switch(protocol) {
            case "file":
                file = url.getFile();
                break;
            case "jar":
                file = new URI(url.getFile().replaceFirst("!.*", "")).toURL().getFile();
                break;
            default:
                throw new IllegalArgumentException("unmanaged ressource URL");
            }
            return Paths.get(file);
        } catch (MalformedURLException | URISyntaxException e) {
            throw new IllegalArgumentException("can't locate the ressource file path", e);
        }
    }

}
