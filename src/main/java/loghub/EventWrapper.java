package loghub;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

class EventWrapper extends Event {
    private final EventInstance event;
    private String[] path;

    public EventWrapper(EventInstance event) {
        this.event = event;
        this.setTimestamp(event.getTimestamp());
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
        if(key instanceof String && "@timestamp".equals(key)) {
            return getTimestamp();
        } else {
            return action( ((i, j, k) -> i.get(j)), key.toString(), null);
        }
    }

    @Override
    public Object remove(Object key) {
        return action( ((i, j, k) -> i.remove(j)), key.toString(), null);
    }

    @Override
    public boolean containsKey(Object key) {
        if(key instanceof String && "@timestamp".equals(key)) {
            return true;
        } else {
            return (Boolean) action( ((i, j, k) -> i.containsKey(j)), key.toString(), null) == true;
        }
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

    @Override
    public Event duplicate() {
        return event.duplicate();
    }

    @Override
    public Processor next() {
        return event.next();
    }

    @Override
    public void appendProcessors(List<Processor> processors) {
        event.appendProcessors(processors);
    }

    @Override
    public void insertProcessor(Processor p) {
        event.insertProcessor(p);
    }

    @Override
    public void appendProcessor(Processor p) {
        event.appendProcessor(p);
    }

    @Override
    public void insertProcessors(List<Processor> p) {
        event.insertProcessors(p);
    }

    @Override
    public boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue) {
        return event.inject(pipeline, mainqueue);
    }

    @Override
    public String getCurrentPipeline() {
        return event.getCurrentPipeline();
    }

    @Override
    public String getNextPipeline() {
        return event.getNextPipeline();
    }

    @Override
    public void process(Processor p) throws ProcessorException {
        p.process(this);
    }

    @Override
    public Date getTimestamp() {
        return event.getTimestamp();
    }

    @Override
    public void setTimestamp(Date timestamp) {
        event.setTimestamp(timestamp);
    }

    @Override
    public ProcessorException buildException(String message) {
        return event.buildException(message);
    }

    @Override
    public ProcessorException buildException(String message, Exception root) {
        return event.buildException(message, root);
    }

}
