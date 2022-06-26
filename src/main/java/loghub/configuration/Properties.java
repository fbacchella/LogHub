package loghub.configuration;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.URIParameter;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import groovy.lang.GroovyClassLoader;
import io.netty.util.concurrent.Future;
import loghub.Dashboard;
import loghub.Event;
import loghub.EventsProcessor;
import loghub.EventsRepository;
import loghub.Expression;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.security.ssl.ClientAuthentication;
import loghub.sources.Source;
import loghub.ThreadBuilder;
import loghub.VarFormatter;
import loghub.metrics.ExceptionsMBean;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.metrics.StatsMBean;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.security.ssl.ContextLoader;
import loghub.security.ssl.MultiKeyStoreProvider;
import loghub.senders.Sender;
import loghub.zmq.ZMQSocketFactory;
import lombok.AllArgsConstructor;

public class Properties extends HashMap<String, Object> {

    enum PROPSNAMES {
        CLASSLOADERNAME,
        GROOVYCLASSLOADERNAME,
        NAMEDPIPELINES,
        FORMATTERS,
        MAINQUEUE,
        OUTPUTQUEUE,
        QUEUESDEPTH,
        PIPELINES,
        RECEIVERS,
        SENDERS,
        SOURCES;

        @Override
        public String toString() {
            return "__" + super.toString();
        }
    }

    @AllArgsConstructor
    private static class FunctionalTimerTask extends TimerTask {
        private final Runnable task;
        @Override
        public void run() {
            task.run();
        }
    }

    public final ClassLoader classloader;
    public final Map<String, Pipeline> namedPipeLine;
    public final Collection<Pipeline> pipelines;
    public final Map<String, Processor> identifiedProcessors;
    public final Collection<Receiver> receivers;
    public final Collection<Sender> senders;
    public final Map<String, Source> sources;
    public final GroovyClassLoader groovyClassLoader;
    public final Map<String, VarFormatter> formatters;
    public final JmxService.Configuration jmxServiceConfiguration;
    public final int numWorkers;
    public final PriorityBlockingQueue mainQueue;
    public final Map<String, BlockingQueue<Event>> outputQueues;
    public final int queuesDepth;
    public final int maxSteps;
    public final EventsRepository<Future<?>> repository;
    public final KeyStore securityStore;
    public final SSLContext ssl;
    public final javax.security.auth.login.Configuration jaasConfig;
    public final JWTHandler jwtHandler;
    public final Dashboard dashboard;
    public final CacheManager cacheManager;
    public final ZMQSocketFactory zSocketFactory;
    public final Set<EventsProcessor> eventsprocessors;

    public final Timer timer = new Timer("loghubtimer", true);

    @SuppressWarnings("unchecked")
    public Properties(Map<String, Object> properties) {
        Stats.reset();
        Expression.clearCache();

        classloader = Optional.ofNullable((ClassLoader) properties.remove(PROPSNAMES.CLASSLOADERNAME.toString()))
                              .orElseGet(Properties.class::getClassLoader);
        groovyClassLoader = Optional.ofNullable((GroovyClassLoader) properties.remove(PROPSNAMES.GROOVYCLASSLOADERNAME.toString()))
                                    .orElseGet(() ->new GroovyClassLoader(classloader));

        if (properties.containsKey("log4j.defaultlevel")) {
            String levelname = (String) properties.remove("log4j.defaultlevel");
            Level log4jlevel = Level.getLevel(levelname);
            LoggerContext ctx = (LoggerContext) LogManager.getContext(classloader, true);
            ctx.getConfiguration().getLoggers().forEach( (i, j) -> j.setLevel(log4jlevel));
            ctx.updateLoggers();
        }

        namedPipeLine = properties.containsKey(PROPSNAMES.NAMEDPIPELINES.toString()) ? (Map<String, Pipeline>) properties.remove(PROPSNAMES.NAMEDPIPELINES.toString()) : Collections.emptyMap();

        pipelines = properties.containsKey(PROPSNAMES.PIPELINES.toString()) ? (Collection<Pipeline>) properties.remove(PROPSNAMES.PIPELINES.toString()) : Collections.emptyList();

        receivers = properties.containsKey(PROPSNAMES.RECEIVERS.toString()) ? (Collection<Receiver>) properties.remove(PROPSNAMES.RECEIVERS.toString()) : Collections.emptyList();

        senders = properties.containsKey(PROPSNAMES.SENDERS.toString()) ? (Collection<Sender>) properties.remove(PROPSNAMES.SENDERS.toString()) : Collections.emptyList();

        Map<String, Processor> _identifiedProcessors = new HashMap<>();
        pipelines.forEach( i-> i.processors.forEach( j -> {
            String id = j.getId();
            if (id != null) {
                _identifiedProcessors.put(id, j);
            }
        }));
        identifiedProcessors = _identifiedProcessors.size() > 0 ? Collections.unmodifiableMap(_identifiedProcessors) : Collections.emptyMap();

        //buffer is here to make writing tests easier
        if (properties.containsKey(PROPSNAMES.FORMATTERS.toString())) {
            formatters = Collections.unmodifiableMap((Map<String, VarFormatter>) properties.remove(PROPSNAMES.FORMATTERS.toString()));
        } else {
            formatters = Collections.emptyMap();
        }

        cacheManager = new CacheManager(this);

        if (properties.containsKey("numWorkers")) {
            numWorkers = (Integer) properties.remove("numWorkers");
        } else {
            numWorkers = Runtime.getRuntime().availableProcessors() * 2;
        }

        if (properties.containsKey("maxSteps")) {
            maxSteps = (Integer) properties.remove("maxSteps");
        } else {
            maxSteps = 128;
        }
        Supplier<KeyStore> sup = () -> Optional.ofNullable(properties.remove("ssl.trusts")).map(this::getKeyStore).orElse(null);
        securityStore = Optional.ofNullable(properties.remove("securityStore")).map(this::getKeyStore).orElseGet(sup);
        if (securityStore != null) {
            properties.put("ssl.trusts", securityStore);
        }
        Map<String, Object> sslprops = properties.entrySet().stream().filter(i -> i.getKey().startsWith("ssl.")).collect(Collectors.toMap( i -> i.getKey().substring(4), Entry::getValue));
        if (! sslprops.isEmpty()) {
            ssl = ContextLoader.build(classloader, sslprops);
            if (ssl == null) {
                throw new ConfigException("SSLContext failed to configure");
            }
        } else {
            ssl = null;
        }

        jwtHandler = buildJwtAlgorithm(filterPrefix(properties, "jwt"));

        zSocketFactory = ZMQSocketFactory.getFactory(filterPrefix(properties, "zmq"));

        javax.security.auth.login.Configuration jc = null;
        if (properties.containsKey("jaasConfig")) {
            String jaasConfigFilePath = (String) properties.remove("jaasConfig");
            URIParameter cp = new URIParameter(Paths.get(jaasConfigFilePath).toUri());
            try {
                jc = javax.security.auth.login.Configuration.getInstance("JavaLoginConfig", cp);
            } catch (NoSuchAlgorithmException e) {
                throw new ConfigException("JavaLoginConfig unavailable", e);
            }
        }
        jaasConfig = jc;

        try {
            jmxServiceConfiguration = JmxService.configuration()
                            .setProperties(filterPrefix(properties, "jmx"))
                            .setSslContext(ssl)
                            .register(StatsMBean.Implementation.NAME, new StatsMBean.Implementation())
                            .register(ExceptionsMBean.Implementation.NAME, new ExceptionsMBean.Implementation())
                            .registerReceivers(receivers)
                            .registerSenders(senders)
                            .registerPipelines(pipelines)
                            .setJaasConfig(jaasConfig);
        } catch (NotCompliantMBeanException | MalformedObjectNameException
                        | InstanceAlreadyExistsException
                        | MBeanRegistrationException ex) {
            throw new ConfigException("Unusuable JMX setup: " + Helpers.resolveThrowableException(ex), ex);
        }

        try {
            dashboard = buildDashboad(filterPrefix(properties, "http"), this);
        } catch (IllegalArgumentException | ExecutionException | InterruptedException e) {
            throw new ConfigException("Failed to build dashboard", e);
        }

        sources = (Map<String, Source>) properties.remove(PROPSNAMES.SOURCES.toString());

        // Default values are for tests, so the build unusable queuing environment
        queuesDepth = properties.containsKey(PROPSNAMES.QUEUESDEPTH.toString()) ? (int) properties.remove(PROPSNAMES.QUEUESDEPTH.toString()) : 0;
        mainQueue = properties.containsKey(PROPSNAMES.MAINQUEUE.toString()) ? (PriorityBlockingQueue) properties.remove(PROPSNAMES.MAINQUEUE.toString()) : new PriorityBlockingQueue();
        outputQueues = properties.containsKey(PROPSNAMES.OUTPUTQUEUE.toString()) ? (Map<String, BlockingQueue<Event>>) properties.remove(PROPSNAMES.OUTPUTQUEUE.toString()) : null;

        Stats.waitingQueue(mainQueue::size);

        // The keys are future
        repository = new EventsRepository<>(this);

        Set<EventsProcessor> allep = new HashSet<>(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            EventsProcessor t = new EventsProcessor(mainQueue, outputQueues, namedPipeLine, maxSteps, repository);
            allep.add(t);
        }
        eventsprocessors = Collections.unmodifiableSet(allep);

        super.putAll(properties);
    }

    private JWTHandler buildJwtAlgorithm(Map<String, Object> properties) {
        Function<Object, String> stringOrNull = k -> (properties.get(k) != null ) ? properties.get(k) .toString() : null;
        return JWTHandler.getBuilder()
                        .secret(stringOrNull.apply("secret")).setAlg(stringOrNull.apply("alg")).build();
    }

    /**
     * Filter a a set of properties, keeping only those starting with the given prefix and removing it.
     * @param input
     * @param prefix
     * @return
     */
    private Map<String, Object> filterPrefix(Map<String, Object> input, String prefix) {
        int prefixLenght = prefix.length() + 1;
        String prefixKey = prefix + ".";
        return input
                        .entrySet()
                        .stream()
                        .filter(i -> i.getKey().startsWith(prefixKey)).collect(Collectors.toMap(i -> i.getKey().substring(prefixLenght), Entry::getValue));
    }

    /**
     * Used by object to register tasks to be executed at defined interval
     * Each task will be given it's own thread at execution.
     * 
     * @param name the name that will be given to the thread when running
     * @param task the task to execute in it's dedicated thread
     * @param period time in milliseconds between successive task executions.
     */
    public void registerScheduledTask(String name, Runnable task, long period) {
        TimerTask collector = new FunctionalTimerTask(() -> 
            ThreadBuilder.get()
                         .setDaemon(true)
                         .setName(name)
                         .setTask(task)
                         .build(true)
        );
        timer.scheduleAtFixedRate(collector, period, period);
    }

    @Override
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public Object replace(String key, Object value) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public Object merge(String key, Object value,
                        BiFunction<? super Object, ? super Object, ? extends Object> remappingFunction) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ? extends Object> function) {
        throw new UnsupportedOperationException("read only");
    }

    private KeyStore getKeyStore(Object trusts) {
        try {
            MultiKeyStoreProvider.SubKeyStore param = new MultiKeyStoreProvider.SubKeyStore();
            if (trusts instanceof Object[]) {
                Arrays.stream((Object[]) trusts).forEach(i -> param.addSubStore(i.toString()));
            } else {
                param.addSubStore(trusts.toString());
            }
            KeyStore ks = KeyStore.getInstance(MultiKeyStoreProvider.NAME, MultiKeyStoreProvider.PROVIDERNAME);
            ks.load(param);
            return ks;
        } catch (KeyStoreException | NoSuchProviderException | NoSuchAlgorithmException | CertificateException | IOException ex) {
            throw new ConfigException("Secret store can’t be used:" + Helpers.resolveThrowableException(ex), ex);
        }
    }

    private Dashboard buildDashboad(Map<String, Object> collect, Properties props)
            throws ExecutionException, InterruptedException {
        Dashboard.Builder builder = Dashboard.getBuilder();
        int port = (Integer) collect.compute("port", (i,j) -> {
            if (j != null && ! (j instanceof Integer)) {
                throw new IllegalArgumentException("http dasbhoard port is not an integer");
            }
            return j != null ? j : -1;
        });
        if (port < 0) {
            return null;
        }
        builder.setPort(port);
        if (Boolean.TRUE.equals(collect.get("withSSL"))) {
            builder.setWithSSL(true);
            String clientAuthentication = collect.compute("SSLClientAuthentication", (i, j) -> j != null ? j : ClientAuthentication.NOTNEEDED).toString();
            String sslKeyAlias = (String) collect.get("SSLKeyAlias");
            builder.setSslKeyAlias(sslKeyAlias).setSslClientAuthentication(ClientAuthentication.valueOf(clientAuthentication.toUpperCase(
                    Locale.ENGLISH)));
        } else {
            builder.setWithSSL(false);
        }
        if ((Boolean)collect.compute("jwt", (i,j) -> Boolean.TRUE.equals(j))) {
            builder.setWithJwt(true).setJwtHandler(props.jwtHandler);
        } else {
            builder.setWithJwt(false);
        }
        String jaasName = collect.compute("jaasName", (i,j) -> j != null ? j : "").toString();
        if (jaasName != null && ! jaasName.isBlank()) {
            builder.setJaasName(jaasName).setJaasConfig(props.jaasConfig);
        }
        return builder.build();
    }

}
