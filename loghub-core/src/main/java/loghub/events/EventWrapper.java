package loghub.events;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;

import loghub.ConnectionContext;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.metrics.Stats.PipelineStat;
import lombok.AccessLevel;
import lombok.Getter;

class EventWrapper extends Event {
    private final Event event;

    @Getter(AccessLevel.PACKAGE)
    private final VariablePath path;

    EventWrapper(Event event) {
        this.event = event;
        this.path = VariablePath.EMPTY;
    }

    EventWrapper(Event event, VariablePath path) {
        // Ensure the path exists as a map
        event.applyAtPath(Action.CHECK_WRAP, path, null, false);
        this.event = event;
        this.path = path;
    }

    @Override
    public Set<java.util.Map.Entry<String, Object>> entrySet() {
        return event.entrySet();
    }

    private Object action(Action f, String key) {
        return action(f, key, null, false);
    }

    private Object action(Action f, String key, Object value, boolean create) {
        VariablePath lpath;
        if (key == null) {
            lpath = path;
        } else if ("^".equals(key)){
            lpath = VariablePath.CURRENT;
        } else if (".".equals(key)){
            lpath = VariablePath.ROOT;
        } else {
            lpath = path.append(key);
        }
        return event.applyAtPath(f, lpath, value, create);
    }

    @Override
    public Object put(String key, Object value) {
        return action(Action.PUT, key, value, true);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        m.forEach(this::put);
    }

    @Override
    public Object get(Object key) {
        return action(Action.GET, key.toString());
    }

    @Override
    public Object remove(Object key) {
        return action(Action.REMOVE, key.toString());
    }

    @Override
    public boolean containsKey(Object key) {
        return Boolean.TRUE.equals(action(Action.CONTAINS, key.toString()));
    }

    @Override
    public String toString() {
        return event.toString();
    }

    @Override
    public int size() {
        return (Integer) action(Action.SIZE, null);
    }

    @Override
    public boolean isEmpty() {
        return (Boolean) action(Action.ISEMPTY, null);
    }

    @Override
    public void clear() {
        action(Action.CLEAR, null);
    }

    @Override
    public boolean containsValue(Object value) {
        return Boolean.TRUE.equals(action(Action.CONTAINSVALUE, null));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<String> keySet() {
        return (Set<String>) action(Action.KEYSET, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<Object> values() {
        return (Collection<Object>) action(Action.VALUES, null);
    }

    @Override
    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        @SuppressWarnings("unchecked")
        Map<String, Object> current = (Map<String, Object>) event.applyAtPath(Action.GET, this.path, null);
        return current.merge(key, value, remappingFunction);
    }

    @Override
    public Event duplicate() throws ProcessorException {
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
    public boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking) {
        return event.inject(pipeline, mainqueue, blocking);
    }

    @Override
    public void reinject(Pipeline pipeline, PriorityBlockingQueue mainqueue) {
        event.reinject(pipeline, mainqueue);
    }

    @Override
    public void reinject(Event ev, PriorityBlockingQueue mainqueue) {
        event.reinject(ev, mainqueue);
    }

    /* (non-Javadoc)
     * @see loghub.Event#refill(loghub.Pipeline)
     */
    @Override
    public void refill(Pipeline pipeline) {
        event.refill(pipeline);
    }

    @Override
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

    public Logger getPipelineLogger() {
        return event.getPipelineLogger();
    }

    @Override
    public Throwable popException() {
        Throwable t = super.popException();
        if (t == null) {
            t = event.popException();
        }
        return t;
    }

}
