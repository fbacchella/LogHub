package loghub.configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

import groovy.lang.GroovyClassLoader;
import loghub.Pipeline;

public class Properties extends HashMap<String, Object> {
    
    final static String CLASSLOADERNAME = "__classloader";
    final static String NAMEDPIPELINES = "__pipelines";
    
    public final ClassLoader classloader;
    public Map<String, Pipeline> namedPipeLine;
    public final GroovyClassLoader groovyClassLoader;
    
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
        super.putAll(properties);
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
