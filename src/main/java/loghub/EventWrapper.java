package loghub;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


public class EventWrapper extends Event {
    private final Event event;
    private String[] path;

    public EventWrapper(Event event) {
        this.event = event;
    }

    public void setProcessor(Processor processor) {
        String[] ppath = processor.getPathArray();
        path = Arrays.copyOf(ppath, ppath.length + 1);
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return event.entrySet();
    }

    private Object action(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String key, final Object value) {
        return action(f, key, value, false);
    }

    private Object action(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String key, final Object value, boolean create) {
        final String[] lpath;
        if(key == null) {
            lpath = path;
        } else if(key.startsWith(".")) {
            String[] tpath = key.substring(1).split(".");
            lpath = tpath.length == 0 ? new String[] {key.substring(1)} : tpath;
        } else {
            path[path.length - 1] = key;
            lpath = path;
        }
        return event.applyAtPath(f, lpath, value, create);
    }

    @Override
    public Object put(String key, Object value) {
        return action( ((i, j, k) -> i.put(j, k)), key, value, true);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        m.entrySet().stream().forEach( i-> { put(i.getKey(), i.getValue()); } );
    }

    @Override
    public Object get(Object key) {
        return action( ((i, j, k) -> i.get(j)), key.toString(), null);
    }

    @Override
    public Object remove(Object key) {
        return action( ((i, j, k) -> i.remove(j)), key.toString(), null);
    }

    @Override
    public boolean containsKey(Object key) {
        return (Boolean) action( ((i, j, k) -> i.containsKey(j)), key.toString(), null) == true;
    }

    @Override
    public String toString() {
        return event.toString();
    }

    public int size() {
        Integer size = (Integer) action( ((i, j, k) -> i.size()), null, null);
        return size != null ? size : 0;
    }

    public boolean isEmpty() {
        return (Boolean) action( ((i, j, k) -> i.isEmpty()), null, null) == true;
    }

    public void clear() {
        action( ((i, j, k) -> {i.clear(); return null;}), null, null);
    }

    public boolean containsValue(Object value) {
        return (Boolean) action( ((i, j, k) -> i.containsValue(k)), null, null) == true;
    }

    @SuppressWarnings("unchecked")
    public Set<String> keySet() {
        Object found = action( ((i, j, k) -> i.keySet()), null, null);
        if(found != null) {
            return (Set<String>) found;
        } else {
            return Collections.emptySet();
        }
    }

    @SuppressWarnings("unchecked")
    public Collection<Object> values() {
        return (Collection<Object>) action( ((i, j, k) -> i.values()), null, null);
    }

}
