package loghub.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class Properties extends HashMap<String, Object> {
    
    final static String CLASSLOADERNAME = "__classloader";
    
    public final ClassLoader classloader;
    
    public Properties(Map<String, Object> properties) {
        super(properties);
        ClassLoader cl = (ClassLoader) properties.get(CLASSLOADERNAME);
        if(cl == null) {
            cl = Properties.class.getClassLoader();
        }
        classloader = cl;
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
