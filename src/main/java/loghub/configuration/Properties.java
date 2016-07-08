package loghub.configuration;

import java.lang.management.ManagementFactory;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

import javax.management.MBeanServer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import groovy.lang.GroovyClassLoader;
import loghub.Event;
import loghub.Pipeline;
import loghub.VarFormatter;
import loghub.jmx.Helper;
import loghub.jmx.Helper.PROTOCOL;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.management.ManagementService;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class Properties extends HashMap<String, Object> {

    private static final Logger logger = LogManager.getLogger();

    final static String CLASSLOADERNAME = "__classloader";
    final static String NAMEDPIPELINES = "__pipelines";
    final static String FORMATTERS = "__formatters";
    final static String MAINQUEUE = "__mainqueue";
    final static String OUTPUTQUEUE = "__outputqueues";
    final static String QUEUESDEPTH = "__queuesdepth";

    public final ClassLoader classloader;
    public Map<String, Pipeline> namedPipeLine;
    public final GroovyClassLoader groovyClassLoader;
    public final Map<String, VarFormatter> formatters;
    public final int jmxport;
    public final PROTOCOL jmxproto;
    public final String jmxlisten;
    public final int numWorkers;
    public final BlockingQueue<Event> mainQueue;
    public final Map<String, BlockingQueue<Event>> outputQueues;
    public final int queuesDepth;

    private final Timer timer = new Timer("loghubtimer", true);
    private final CacheManager cacheManager;


    @SuppressWarnings("unchecked")
    public Properties(Map<String, Object> properties) {
        super();

        String tz = (String) properties.remove("timezone");
        try {
            if (tz != null) {
                ZoneId id = ZoneId.of(tz);
                TimeZone.setDefault(TimeZone.getTimeZone(id));
            }
        } catch (DateTimeException e) {
            logger.error("Invalid timezone {}: {}", tz, e.getMessage());
        }

        String locale = (String) properties.remove("locale");
        if(locale != null) {
            Locale l = Locale.forLanguageTag(locale);
            Locale.setDefault(l);
        }

        ClassLoader cl = (ClassLoader) properties.remove(CLASSLOADERNAME);
        if(cl == null) {
            cl = Properties.class.getClassLoader();
        }
        classloader = cl;

        namedPipeLine = (Map<String, Pipeline>) properties.remove(NAMEDPIPELINES);
        if(namedPipeLine != null) {
            namedPipeLine = Collections.unmodifiableMap(namedPipeLine);
        } else {
            namedPipeLine = Collections.emptyMap();
        }

        groovyClassLoader = new GroovyClassLoader(cl);

        cacheManager = new CacheManager(new net.sf.ehcache.config.Configuration()
                .name(UUID.randomUUID().toString())
                );

        //buffer is here to make writing tests easier
        Map<String, String> buffer = (Map<String, String>) properties.remove(FORMATTERS);
        if(buffer != null && buffer.size() > 0) {
            Map<String, VarFormatter> formatters = new HashMap<>(buffer.size());
            buffer.entrySet().stream().forEach(i -> formatters.put(i.getKey(), new VarFormatter(i.getValue())));
            this.formatters = Collections.unmodifiableMap(formatters);
        } else {
            formatters = Collections.emptyMap();

        }

        //Read the jmx configuration
        Integer jmxport = (Integer) properties.get("jmx.port");
        if(jmxport != null) {
            this.jmxport = jmxport;
        } else {
            this.jmxport = -1;
        }
        String jmxproto = (String) properties.get("jmx.protocol");
        if(jmxproto != null) {
            this.jmxproto = PROTOCOL.valueOf(jmxproto.toLowerCase());
        } else {
            this.jmxproto = Helper.defaultProto;
        }
        String jmxlisten = (String) properties.get("jmx.listen");
        if(jmxlisten != null) {
            this.jmxlisten = jmxlisten;
        } else {
            this.jmxlisten = "0.0.0.0";
        }

        if(properties.containsKey("numWorkers")) {
            numWorkers = (Integer) properties.get("numWorkers");
        } else {
            numWorkers = Runtime.getRuntime().availableProcessors() * 2;
        }

        if(this.jmxport > 0) {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer(); 
            ManagementService.registerMBeans(cacheManager, mBeanServer, false, false, false, true);
        }

        // Default values are for tests, so the build unusable queuing environment
        queuesDepth = properties.containsKey(QUEUESDEPTH) ? (int) properties.get(QUEUESDEPTH) : 0;
        mainQueue = properties.containsKey(QUEUESDEPTH) ? (BlockingQueue<Event>) properties.get(MAINQUEUE) : null;
        outputQueues = properties.containsKey(QUEUESDEPTH) ? (Map<String, BlockingQueue<Event>>) properties.get(OUTPUTQUEUE) : null;

        super.putAll(properties);
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
                Thread taskexecution = new Thread(task, name);
                taskexecution.setDaemon(true);
                taskexecution.start();
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
