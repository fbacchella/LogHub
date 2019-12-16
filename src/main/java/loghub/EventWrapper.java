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
import java.util.stream.Stream;

import loghub.Stats.PipelineStat;

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

    private Object action(Action f, String key, final Object value) throws ProcessorException {
        return action(f, key, value, false);
    }

    private Object action(Action f, String key, final Object value, boolean create) throws ProcessorException {
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
        try {
            return action(Action.PUT, key, value, true);
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> m) {
        m.entrySet().stream().forEach( i->  put(i.getKey(), i.getValue()) );
    }

    @Override
    public Object get(Object key) {
        try {
            return action(Action.GET, key.toString(), null);
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public Object remove(Object key) {
        try {
            return action(Action.REMOVE, key.toString(), null);
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            return Boolean.TRUE.equals(action(Action.CONTAINS, key.toString(), null));
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public String toString() {
        return event.toString();
    }

    @Override
    public int size() {
        Integer size;
        try {
            size = (Integer) action( Action.SIZE, null, null);
            return size != null ? size : 0;
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return (Boolean) action(Action.ISEMPTY, null, null) == true;
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public void clear() {
        try {
            action(Action.CLEAR, null, null);
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @Override
    public boolean containsValue(Object value) {
        try {
            return Boolean.TRUE.equals(action(Action.CONTAINSVALUE, null, null));
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> keySet() {
        try {
            Object found = action(Action.KEYSET, null, null);
            if(found != null) {
                return (Set<String>) found;
            } else {
                return Collections.emptySet();
            }
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Object> values() {
        try {
            return (Collection<Object>) action( Action.VALUES, null, null);
        } catch (ProcessorException e) {
            throw new UncheckedProcessorException(e);
        }
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
    public int processingDone() {
        return event.processingDone();
    }

    @Override
    public int processingLeft() {
        return event.processingLeft();
    }

    @Override
    public boolean isTest() {
        return event.isTest();
    }

    @Override
    public void doMetric(PipelineStat status, Throwable ex) {
        event.doMetric(status, ex);
    }

    @Override
    public void drop() {
        event.drop();
    }

    @Override
    public <T> ConnectionContext<T> getConnectionContext() {
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

    @Override
    public Stream<Entry<String, Object>> getMetaAsStream() {
        return event.getMetaAsStream();
    }

}
