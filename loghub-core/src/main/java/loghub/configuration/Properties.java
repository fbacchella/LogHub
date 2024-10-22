package loghub.configuration;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import javax.management.NotCompliantMBeanException;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import com.sun.management.HotSpotDiagnosticMXBean;

import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.Future;
import loghub.Dashboard;
import loghub.EventsProcessor;
import loghub.EventsRepository;
import loghub.Expression;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ThreadBuilder;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.ExceptionsMBean;
import loghub.metrics.JmxService;
import loghub.metrics.Stats;
import loghub.metrics.StatsMBean;
import loghub.receivers.Receiver;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import loghub.security.ssl.SslContextBuilder;
import loghub.senders.Sender;
import loghub.sources.Source;
import loghub.zmq.ZMQSocketFactory;
import lombok.AllArgsConstructor;

public class Properties extends HashMap<String, Object> {

    enum PROPSNAMES {
        JWTHANDLER,
        JAASCONFIG,
        SSLCONTEXT,
        SSLCONTEXTBUILDER,
        CLASSLOADERNAME,
        GROOVYCLASSLOADERNAME,
        NAMEDPIPELINES,
        MAINQUEUE,
        OUTPUTQUEUE,
        QUEUESDEPTH,
        PIPELINES,
        RECEIVERS,
        SENDERS,
        SOURCES,
        CACHEMANGER;

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

    private static class ZMQFactoryReference {
        private ZMQSocketFactory.ZMQSocketFactoryBuilder builder = null;
        private ZMQSocketFactory factory = null;
    }

    public final ClassLoader classloader;
    public final Map<String, Pipeline> namedPipeLine;
    public final Collection<Pipeline> pipelines;
    public final Map<String, Processor> identifiedProcessors;
    public final Collection<Receiver> receivers;
    public final Collection<Sender> senders;
    public final Map<String, Source> sources;
    public final JmxService.Configuration jmxServiceConfiguration;
    public final int numWorkers;
    public final PriorityBlockingQueue mainQueue;
    public final Map<String, BlockingQueue<Event>> outputQueues;
    public final int queuesDepth;
    public final int maxSteps;
    public final EventsRepository<Future<?>> repository;
    public final SslContextBuilder sslBuilder;
    public final SSLContext ssl;
    public final javax.security.auth.login.Configuration jaasConfig;
    public final JWTHandler jwtHandler;
    public final Dashboard dashboard;
    public final CacheManager cacheManager;
    public final Set<EventsProcessor> eventsprocessors;
    public final Runnable hprofdump;
    public final EventsFactory eventsFactory = new EventsFactory();
    public final HashedWheelTimer processExpiration = new HashedWheelTimer(ThreadBuilder.get().setDaemon(true).getFactory("EventsRepository-timeoutmanager"));

    public final Timer timer = new Timer("loghubtimer", true);

    private final Set<Runnable> shutdownTasks = new HashSet<>();
    private final AtomicReference<ZMQFactoryReference> zmqFactoryReference = new AtomicReference<>(new ZMQFactoryReference());
    private final Set<EventsRepository<?>> repositories = new HashSet<>();

    @SuppressWarnings("unchecked")
    public Properties(Map<String, Object> properties) {
        Stats.reset();
        Expression.clearCache();
        processExpiration.start();

        classloader = Optional.ofNullable((ClassLoader) properties.remove(PROPSNAMES.CLASSLOADERNAME.toString()))
                              .orElseGet(Properties.class::getClassLoader);
        cacheManager = Optional.ofNullable((CacheManager) properties.remove(PROPSNAMES.CACHEMANGER.toString()))
                                    .orElseGet(() -> new CacheManager(classloader));


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
        identifiedProcessors = !_identifiedProcessors.isEmpty() ? Collections.unmodifiableMap(_identifiedProcessors) : Collections.emptyMap();

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

        sslBuilder = Optional.ofNullable((SslContextBuilder) properties.get(PROPSNAMES.SSLCONTEXTBUILDER.toString()))
                             .orElseGet(SslContextBuilder::getBuilder);
        ssl = Optional.ofNullable((SSLContext) properties.get(PROPSNAMES.SSLCONTEXT.toString()))
                      .orElseGet(sslBuilder::build);
        jaasConfig = (javax.security.auth.login.Configuration) properties.get(PROPSNAMES.JAASCONFIG.toString());
        jwtHandler = (JWTHandler) properties.get(PROPSNAMES.JWTHANDLER.toString());

        zmqFactoryReference.get().builder = getFactory(Helpers.filterPrefix(properties, "zmq"));

        try {
            jmxServiceConfiguration = JmxService.configuration()
                            .setProperties(Helpers.filterPrefix(properties, "jmx"))
                            .setSslContext(sslBuilder.build())
                            .register(StatsMBean.Implementation.NAME, new StatsMBean.Implementation())
                            .register(ExceptionsMBean.Implementation.NAME, new ExceptionsMBean.Implementation())
                            .registerReceivers(receivers)
                            .registerSenders(senders)
                            .registerPipelines(pipelines)
                            .setJaasConfig(jaasConfig);
        } catch (NotCompliantMBeanException ex) {
            throw new ConfigException("Unusable JMX setup: " + Helpers.resolveThrowableException(ex), ex);
        }

        dashboard = buildDashboard(Helpers.filterPrefix(properties, "http"));

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

        hprofdump = properties.containsKey("hprofDumpPath") ? makeHprofDump((String)properties.remove("hprofDumpPath")) : () -> {};

        super.putAll(properties);

        VariablePath.compact();
    }

    private Runnable makeHprofDump(String hprofFile) {
        Semaphore firstCrash = new Semaphore(1);
        HotSpotDiagnosticMXBean diagnosticMBean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        Path hprofPath = Path.of(hprofFile);
        Logger logger = LogManager.getLogger("loghub");
        return () -> {
            // Only dump once
           if (firstCrash.tryAcquire()) {
               logger.warn("Dumping jvm content to " + hprofFile);
                try {
                    Files.deleteIfExists(hprofPath);
                    diagnosticMBean.dumpHeap(hprofFile, true);
                } catch (Exception ex) {
                    logger.error("Unable to dump hprof: {}", () -> Helpers.resolveThrowableException(ex));
                }
            }
        };
    }

    public void registerEventsRepository(EventsRepository<?> repository) {
        repositories.add(repository);
    }

    public Set<EventsRepository<?>> eventsRepositories() {
        return Collections.unmodifiableSet(repositories);
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
    public void putAll(Map<? extends String, ?> m) {
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
                        BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("read only");
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        throw new UnsupportedOperationException("read only");
    }

    public void terminate() {
        shutdownTasks.forEach(Runnable::run);
    }

    public Runnable terminator() {
        return () -> shutdownTasks.forEach(Runnable::run);
    }

    private Dashboard buildDashboard(Map<String, Object> collect) {
        Dashboard.Builder builder = Dashboard.getBuilder();
        builder.setClassLoader(classloader);
        int port = ((Number) collect.compute("port", (i, j) -> {
            if (j != null && ! (j instanceof Number)) {
                throw new IllegalArgumentException("HTTP dashboard port is not an integer");
            }
            return j != null ? j : -1;
        })).intValue();
        if (port < 0) {
            return null;
        }
        builder.setPort(port);
        if (collect.containsKey("listen")) {
            builder.setListen(collect.get("listen").toString());
        }
        if (Boolean.TRUE.equals(collect.get("withSSL"))) {
            builder.setWithSSL(true);
            builder.setSslContext(Optional.ofNullable(collect.get("sslContext")).map(SSLContext.class::cast).orElse(ssl));
            builder.setSslParams(Optional.ofNullable(collect.get("sslParams")).map(SSLParameters.class::cast).orElse(null));
            String clientAuthentication = collect.compute("SSLClientAuthentication", (i, j) -> j != null ? j : ClientAuthentication.NOTNEEDED).toString();
            String sslKeyAlias = (String) collect.get("SSLKeyAlias");
            builder.setSslKeyAlias(sslKeyAlias)
                   .setSslClientAuthentication(ClientAuthentication.valueOf(clientAuthentication.toUpperCase(Locale.ENGLISH)));
            builder.setHstsDuration(Optional.ofNullable(collect.get("hstsDuration"))
                                            .map(String.class::cast)
                                            .map(Duration::parse)
                                            .orElse(null)
            );
        } else {
            builder.setWithSSL(false);
        }
        if (Boolean.TRUE.equals(collect.compute("jwt", (i,j) -> Boolean.TRUE.equals(j)))) {
            builder.setWithJwtUrl(true).setJwtHandlerUrl(jwtHandler);
        } else {
            builder.setWithJwtUrl(false);
        }
        String jaasName = collect.compute("jaasName", (i,j) -> (j != null) ? j : "").toString();
        if (jaasName != null && ! jaasName.isBlank()) {
            builder.setJaasNameJwt(jaasName).setJaasConfigJwt(jaasConfig);
        }
        if (Boolean.TRUE.equals(collect.compute("withJolokia", (i,j) -> Boolean.TRUE.equals(j)))) {
            builder.setWithJolokia(true);
            if (collect.containsKey("jolokiaPolicyLocation")) {
                builder.setJolokiaPolicyLocation(collect.get("jolokiaPolicyLocation").toString());
            }
        } else {
            builder.setWithJolokia(false);
        }
        return builder.build();
    }

    private ZMQSocketFactory.ZMQSocketFactoryBuilder getFactory(Map<String, Object> properties) {
        ZMQSocketFactory.ZMQSocketFactoryBuilder builder = ZMQSocketFactory.builder();
        Optional.ofNullable(properties.remove("keystore")).map(String.class::cast).map(Paths::get).ifPresent(builder::zmqKeyStore);
        Optional.ofNullable(properties.remove("certsDirectory")).map(String.class::cast).map(Paths::get).ifPresent(builder::zmqCertsDir);
        Optional.ofNullable(properties.remove("withZap")).map(Boolean.class::cast).ifPresent(builder::withZap);
        Optional.ofNullable(properties.remove("numSocket")).map(Integer.class::cast).ifPresent(builder::numSocket);
        Optional.ofNullable(properties.remove("linger")).map(Integer.class::cast).ifPresent(builder::linger);
        return builder;
    }

    public ZMQSocketFactory getZMQSocketFactory() {
        return zmqFactoryReference.updateAndGet(f -> {
            if (f.factory == null) {
                f.factory = f.builder.build();
                f.builder = null;
                shutdownTasks.add(f.factory::close);
                f.factory.setExceptionHandler(ThreadBuilder.DEFAULTUNCAUGHTEXCEPTIONHANDLER);
            }
            return f;
        }).factory;
    }

}
