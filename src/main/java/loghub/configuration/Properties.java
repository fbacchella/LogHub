package loghub.configuration;

import java.lang.management.ManagementFactory;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.URIParameter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.DefaultObjectNameFactory;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jmx.ObjectNameFactory;

import groovy.lang.GroovyClassLoader;
import io.netty.util.concurrent.Future;
import loghub.DashboardHttpServer;
import loghub.Event;
import loghub.EventsRepository;
import loghub.PausingTimer;
import loghub.Pipeline;
import loghub.Processor;
import loghub.Receiver;
import loghub.Sender;
import loghub.Source;
import loghub.ThreadBuilder;
import loghub.VarFormatter;
import loghub.jmx.Helper;
import loghub.jmx.Helper.PROTOCOL;
import loghub.security.JWTHandler;
import loghub.security.ssl.ContextLoader;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.management.ManagementService;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class Properties extends HashMap<String, Object> {

    public static final class MetricRegistryWrapper {
        private MetricRegistry metrics = new MetricRegistry();

        public Counter counter(String name) {
            return metrics.counter(name);
        }

        public Histogram histogram(String name) {
            return metrics.histogram(name);
        }

        public Meter meter(String name) {
            return metrics.meter(name);
        }

        public com.codahale.metrics.Timer timer(String name) {
            return metrics.timer(name);
        }

        public PausingTimer pausingTimer(String name) {
            return (PausingTimer) metrics.timer(name, PausingTimer.pausingsupplier);
        }

        public void reset() {
            metrics = new MetricRegistry();
        }

        public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException {
            return metrics.register(name, metric);
        }

        public JmxReporter getJmxReporter() {
            ObjectNameFactory donf = new DefaultObjectNameFactory();
            final Pattern pipepattern = Pattern.compile("([^\\.]+)\\.([^\\.]+)\\.(.*)");
            return JmxReporter.forRegistry(metrics).createsObjectNamesWith(new ObjectNameFactory() {
                @Override
                public ObjectName createName(String type, String domain, String name) {
                    Matcher m = pipepattern.matcher(name);
                    if (m.matches()) {
                        String service = m.group(1);
                        String servicename = m.group(2);
                        String metric = m.group(3);
                        try {
                            ObjectName on = new ObjectName(String.format("loghub:type=%s,servicename=%s,name=%s", service, servicename, metric));
                            return on;
                        } catch (MalformedObjectNameException e) {
                            return donf.createName(type, domain, name);
                        }
                    } else {
                        return donf.createName(type, domain, name);
                    }
                }

            }).build();
        }
    };

    public static final MetricRegistryWrapper metrics = new MetricRegistryWrapper();

    enum PROPSNAMES {
        CLASSLOADERNAME,
        NAMEDPIPELINES,
        FORMATTERS,
        MAINQUEUE,
        OUTPUTQUEUE,
        QUEUESDEPTH,
        PIPELINES,
        RECEIVERS,
        SENDERS,
        TOPPIPELINE,
        SOURCES;
        @Override
        public String toString() {
            return "__" + super.toString();
        }
    };

    public final ClassLoader classloader;
    public final Map<String, Pipeline> namedPipeLine;
    public final Collection<Pipeline> pipelines;
    public final Map<String, Processor> identifiedProcessors;
    public final Collection<Receiver> receivers;
    public final Collection<Sender> senders;
    public final Map<String, Source> sources;
    public final GroovyClassLoader groovyClassLoader;
    public final Map<String, VarFormatter> formatters;
    public final int jmxport;
    public final PROTOCOL jmxproto;
    public final String jmxlisten;
    public final int numWorkers;
    public final BlockingQueue<Event> mainQueue;
    public final Map<String, BlockingQueue<Event>> outputQueues;
    public final int queuesDepth;
    public final int maxSteps;
    public final EventsRepository<Future<?>> repository;
    public final SSLContext ssl;
    public final javax.security.auth.login.Configuration jaasConfig;
    public final JWTHandler jwtHandler;
    public final DashboardHttpServer.Builder dashboardBuilder;

    public final Timer timer = new Timer("loghubtimer", true);
    private final CacheManager cacheManager;

    @SuppressWarnings("unchecked")
    public Properties(Map<String, Object> properties) {
        super();

        metrics.reset();

        ClassLoader cl = (ClassLoader) properties.remove(PROPSNAMES.CLASSLOADERNAME.toString());
        if (cl == null) {
            cl = Properties.class.getClassLoader();
        }
        classloader = cl;

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

        groovyClassLoader = new GroovyClassLoader(cl);

        cacheManager = new CacheManager(new net.sf.ehcache.config.Configuration()
                .name(UUID.randomUUID().toString())
                );

        Map<String, Processor> _identifiedProcessors = new HashMap<String, Processor>();
        pipelines.forEach( i-> i.processors.forEach( j -> {
            String id = j.getId();
            if (id != null) {
                _identifiedProcessors.put(id, j);
            }
        }));
        identifiedProcessors = _identifiedProcessors.size() > 0 ? Collections.unmodifiableMap(_identifiedProcessors) : Collections.emptyMap();

        //buffer is here to make writing tests easier
        Map<String, String> buffer = (Map<String, String>) properties.remove(PROPSNAMES.FORMATTERS.toString());
        if (buffer != null && buffer.size() > 0) {
            Map<String, VarFormatter> formattersMap = new HashMap<>(buffer.size());
            buffer.entrySet().stream().forEach(i -> formattersMap.put(i.getKey(), new VarFormatter(i.getValue())));
            formatters = Collections.unmodifiableMap(formattersMap);
        } else {
            formatters = Collections.emptyMap();
        }

        // Extracts all the named pipelines and generate metrics for them
        namedPipeLine.keySet().stream().forEach( i -> {
            metrics.counter("Pipeline." + i + ".inflight");
            metrics.pausingTimer("Pipeline." + i + ".timer");
            metrics.meter("Pipeline." + i + ".failed");
            metrics.meter("Pipeline." + i + ".dropped");
            metrics.meter("Pipeline." + i + ".blocked.in");
            metrics.meter("Pipeline." + i + ".blocked.out");
        });
        metrics.counter("Allevents.inflight");
        metrics.timer("Allevents.timer");
        //Read the jmx configuration
        Integer jmxport = (Integer) properties.remove("jmx.port");
        if (jmxport != null) {
            this.jmxport = jmxport;
        } else {
            this.jmxport = -1;
        }
        String jmxproto = (String) properties.remove("jmx.protocol");
        if (jmxproto != null) {
            this.jmxproto = PROTOCOL.valueOf(jmxproto.toLowerCase());
        } else {
            this.jmxproto = Helper.DEFAULTPROTOCOL;
        }
        String jmxlisten = (String) properties.remove("jmx.listen");
        if (jmxlisten != null) {
            this.jmxlisten = jmxlisten;
        } else {
            this.jmxlisten = "0.0.0.0";
        }

        if (this.jmxport > 0) {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer(); 
            ManagementService.registerMBeans(cacheManager, mBeanServer, false, false, false, true);
        }

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

        ssl = ContextLoader.build(properties.entrySet().stream().filter(i -> i.getKey().startsWith("ssl.")).collect(Collectors.toMap( i -> i.getKey().substring(4), j -> j.getValue())));

        jwtHandler = buildJwtAlgorithm(properties.entrySet().stream().filter(i -> i.getKey().startsWith("jwt.")).collect(Collectors.toMap( i -> i.getKey().substring(4), j -> j.getValue())));

        javax.security.auth.login.Configuration jc = null;
        if (properties.containsKey("jaasConfig")) {
            String jaasConfigFilePath = (String) properties.remove("jaasConfig");
            URIParameter cp = new URIParameter(Paths.get(jaasConfigFilePath).toUri());
            try {
                jc = javax.security.auth.login.Configuration.getInstance("JavaLoginConfig", cp);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("JavaLoginConfig unavailable", e);
            }
        }
        jaasConfig = jc;

        dashboardBuilder = DashboardHttpServer.buildDashboad(properties.entrySet().stream().filter(i -> i.getKey().startsWith("http.")).collect(Collectors.toMap( i -> i.getKey().substring(5), j -> j.getValue())), this);

        sources = (Map<String, Source>) properties.remove(PROPSNAMES.SOURCES.toString());

        // Default values are for tests, so the build unusable queuing environment
        queuesDepth = properties.containsKey(PROPSNAMES.QUEUESDEPTH.toString()) ? (int) properties.remove(PROPSNAMES.QUEUESDEPTH.toString()) : 0;
        mainQueue = properties.containsKey(PROPSNAMES.MAINQUEUE.toString()) ? (BlockingQueue<Event>) properties.remove(PROPSNAMES.MAINQUEUE.toString()) :  new LinkedBlockingDeque<Event>();;
        outputQueues = properties.containsKey(PROPSNAMES.OUTPUTQUEUE.toString()) ? (Map<String, BlockingQueue<Event>>) properties.remove(PROPSNAMES.OUTPUTQUEUE.toString()) : null;

        metrics.register(
                "EventWaiting.mainloop",
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return mainQueue != null ? mainQueue.size() : 0;
                    }
                });

        if (outputQueues != null) {
            for(Map.Entry<String, BlockingQueue<Event>> i: outputQueues.entrySet()) {
                final BlockingQueue<Event> queue = i.getValue();
                final String name = i.getKey();
                metrics.register(
                        "EventWaiting.output." + name,
                        new Gauge<Integer>() {
                            @Override
                            public Integer getValue() {
                                return queue.size();
                            }
                        });
            }
        }

        repository = new EventsRepository<Future<?>>(this);

        super.putAll(properties);
    }

    private JWTHandler buildJwtAlgorithm(Map<String, Object> properties) {
        Function<Object, String> stringOrNull = k -> (properties.get(k) != null ) ? properties.get(k) .toString() : null;
        return JWTHandler.getBuilder()
                .secret(stringOrNull.apply("secret")).setAlg(stringOrNull.apply("alg")).build();
    }

    public Cache getCache(int size, String name, Object parent) {
        CacheConfiguration config = getDefaultCacheConfig(name, parent).maxEntriesLocalHeap(size);
        return getCache(config);
    }

    public CacheConfiguration getDefaultCacheConfig(String name, Object parent) {
        return new CacheConfiguration()
                .name(name + "@" + parent.hashCode())
                .persistence(new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE))
                .eternal(true)
                .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.LFU)
                ;
    }

    public Cache getCache(CacheConfiguration config) {
        Cache memoryOnlyCache = new Cache(config); 
        cacheManager.addCache(memoryOnlyCache); 
        return memoryOnlyCache;
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
        TimerTask collector = new TimerTask () {
            public void run() {
                ThreadBuilder.get()
                .setDaemon(true)
                .setName(name)
                .setRunnable(task)
                .build(true);
            }
        };
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
    public void replaceAll(
            BiFunction<? super String, ? super Object, ? extends Object> function) {
        throw new UnsupportedOperationException("read only");
    }

}
