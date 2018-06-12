package loghub;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;

class EventWrapper extends Event {
    private final Event event;
    private String[] path;

    EventWrapper(Event event) {
        this.event = event;
        this.setTimestamp(event.getTimestamp());
    }

    EventWrapper(Event event, String[] path) {
        this.event = event;
        this.setTimestamp(event.getTimestamp());
        this.path = Arrays.copyOf(path, path.length + 1);
    }

    public void setProcessor(Processor processor) {
        String[] ppath = processor.getPathArray();
        path = Arrays.copyOf(ppath, ppath.length + 1);
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return event.entrySet();
    }

    private Object action(Action f, String key, final Object value) {
        return action(f, key, value, false);
    }

    private Object action(Action f, String key, final Object value, boolean create) {
        final String[] lpath;
        if(key == null) {
            lpath = path;
        } else if(key.startsWith(".")) {
            String[] tpath = key.substring(1).split(".");
            lpath = tpath.length == 0 ? new String[] {key.substring(1)} : tpath;
        } else if(key.startsWith("@") || key.startsWith("#")) {
            // If key is a meta, don't append the path
            lpath = new String[] {key};
        } else {
            path[path.length - 1] = key;
            lpath = path;
        }
        return event.applyAtPath(f, lpath, value, create);
    }

    @Override
    public Object put(String key, Object value) {
        return action( Action.PUT, key, value, true);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        m.entrySet().stream().forEach( i->  put(i.getKey(), i.getValue()) );
    }

    @Override
    public Object get(Object key) {
        return action(Action.GET, key.toString(), null);
    }

    @Override
    public Object remove(Object key) {
        return action(Action.REMOVE, key.toString(), null);
    }

    @Override
    public boolean containsKey(Object key) {
        return Boolean.TRUE.equals(action(Action.CONTAINS, key.toString(), null));
    }

    @Override
    public String toString() {
        return event.toString();
    }

    @Override
    public int size() {
        Integer size = (Integer) action( Action.SIZE, null, null);
        return size != null ? size : 0;
    }

    @Override
    public boolean isEmpty() {
        return (Boolean) action(Action.ISEMPTY, null, null) == true;
    }

    @Override
    public void clear() {
        action(Action.CLEAR, null, null);
    }

    @Override
    public boolean containsValue(Object value) {
        return Boolean.TRUE.equals(action(Action.CONTAINSVALUE, null, null));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> keySet() {
        Object found = action(Action.KEYSET, null, null);
        if(found != null) {
            return (Set<String>) found;
        } else {
            return Collections.emptySet();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Object> values() {
        return (Collection<Object>) action( Action.VALUES, null, null);
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
    public boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue, boolean blocking) {
        return event.inject(pipeline, mainqueue, blocking);
    }

    /* (non-Javadoc)
     * @see loghub.Event#refill(loghub.Pipeline)
     */
    @Override
    public void refill(Pipeline pipeline) {
        event.refill(pipeline);
    }

    public boolean inject(Event ev, BlockingQueue<Event> mainqueue) {
        return event.inject(ev, mainqueue);
    }

    public void finishPipeline() {
        event.finishPipeline();
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
    public boolean process(Processor p) throws ProcessorException {
        return p.process(this);
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
    public void end() {
        event.end();
    }

    @Override
    public int stepsCount() {
        return event.stepsCount();
    }

    @Override
    public boolean isTest() {
        return event.isTest();
    }

    @Override
    public void doMetric(Runnable metric) {
        event.doMetric(metric);
    }

    @Override
    public void drop() {
        event.drop();
    }

    @Override
    public ConnectionContext<?> getConnectionContext() {
        return event.getConnectionContext();
    }

    @Override
    protected EventInstance getRealEvent() {
        return event.getRealEvent();
    }

    @Override
    public Event unwrap() {
        return event;
    }


    @Override
    public Map<String, Object> getMetas() {
        return event.getMetas();
    }

    @Override
    public void mergeMeta(Event event,
                          BiFunction<Object, Object, Object> cumulator) {
        event.mergeMeta(event, cumulator);
    }

    @Override
    public Object getMeta(String key) {
        return event.getMeta(key);
    }

    @Override
    public Object putMeta(String key, Object value) {
        return event.putMeta(key, value);
    }

}
