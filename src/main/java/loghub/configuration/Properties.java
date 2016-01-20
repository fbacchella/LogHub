package loghub.configuration;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import javax.management.MBeanServer;

import groovy.lang.GroovyClassLoader;
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

    final static String CLASSLOADERNAME = "__classloader";
    final static String NAMEDPIPELINES = "__pipelines";
    final static String FORMATTERS = "__formatters";

    public final ClassLoader classloader;
    public Map<String, Pipeline> namedPipeLine;
    public final GroovyClassLoader groovyClassLoader;
    private final CacheManager cacheManager;
    final Map<String, VarFormatter> formatters = new HashMap<String, VarFormatter>();
    public final int jmxport;
    public final PROTOCOL jmxproto;
    public final String jmxlisten;

    @SuppressWarnings("unchecked")
    public Properties(Map<String, Object> properties) {
        super();
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
        Map<String, VarFormatter> buffer = (Map<String, VarFormatter>) properties.remove(FORMATTERS);
        if(buffer != null && buffer.size() > 0) {
            formatters.putAll(buffer);
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

        if(this.jmxport > 0) {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer(); 
            ManagementService.registerMBeans(cacheManager, mBeanServer, false, false, false, 
                    true);
        }

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
