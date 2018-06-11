package loghub;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
public abstract class Event extends HashMap<String, Object> implements Serializable {

    public static final String TIMESTAMPKEY = "@timestamp";
    public static final String CONTEXTKEY = "@context";

    public enum Action {
        GET((i,j,k) -> i.get(j)),
        PUT((i, j, k) -> i.put(j, k)),
        REMOVE((i, j, k) -> i.remove(j)),
        CONTAINS((i, j, k) -> i.containsKey(j)),
        SIZE((i, j, k) -> i.size()),
        ISEMPTY((i, j, k) -> i.isEmpty()),
        CLEAR((i, j, k) -> {i.clear(); return null;}),
        CONTAINSVALUE((i, j, k) -> i.containsValue(k)),
        KEYSET((i, j, k) -> i.keySet()),
        VALUES((i, j, k) -> i.values()),
        ;
        public final Helpers.TriFunction<Map<String, Object>, String, Object, Object> action;
        Action (Helpers.TriFunction<Map<String, Object>, String, Object, Object> action){
            this.action = action;
        }
    }

    public static Event emptyEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx);
    }

    public static Event emptyTestEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx, true);
    }

    public Object applyAtPath(Action f, String[] path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    @SuppressWarnings("unchecked")
    public Object applyAtPath(Action f, String[] path, Object value, boolean create) {
        Map<String, Object> current = this;
        String key = path[0];
        if (key != null && key.startsWith("#")) {
            return f.action.apply(getMetas(), key.substring(1), value);
        } else if ( TIMESTAMPKEY.equals(key)) {
            switch(f) {
            case GET: return getTimestamp();
            case PUT: { 
                if (value instanceof Date) {
                    setTimestamp((Date) value);
                } else if (value instanceof Number){
                    Date newDate = new Date(((Number)value).longValue());
                    setTimestamp(newDate);
                }
                return null;
            }
            case REMOVE: return getTimestamp();
            case CONTAINS: return true;
            case SIZE: return 1;
            case ISEMPTY: return false;
            case CLEAR: return null;
            case CONTAINSVALUE: return getTimestamp().equals(value);
            case KEYSET: return Collections.singleton(TIMESTAMPKEY);
            case VALUES: return Collections.singleton(getTimestamp());
            default: return null;
            }
        } else {
            int startwalk = 0;
            if (".".equals(path[0])) {
                current = getRealEvent();
                key=path[1];
                startwalk = 1;
            }
            for (int i = startwalk ; i < path.length - 1; i++) {
                Object peekNext = current.get(key);
                Map<String, Object> next;
                if ( peekNext == null ) {
                    if (create) {
                        next = new HashMap<>();
                        current.put(path[i], next);
                    } else {
                        return null;
                    }
                } else if ( ! (peekNext instanceof Map) ) {
                    throw new UncheckedProcessingException(getRealEvent(), "Can descend into " + key + ", it's not an object");
                } else {
                    next = (Map<String, Object>) peekNext;
                }
                current = next;
                key = path[i + 1];
            }
            return f.action.apply(current, key, value);
        }
    }

    protected abstract Map<String, Object> getMetas();

    public abstract Object getMeta(String key);

    public abstract Object putMeta(String key, Object value);

    public ProcessorException buildException(String message) {
        return new ProcessorException(getRealEvent(), message);
    }

    public ProcessorException buildException(String message, Exception root) {
        return new ProcessorException(getRealEvent(), message, root);
    }

    public Object getPath(String...path) {
        return applyAtPath(Action.GET, path, null, false);
    }

    /**
     * This method inject a new event in a pipeline as
     * a top processing pipeline. Not to be used for sub-processing pipeline. It can only
     * be used for a new event.
     * 
     * @param pipeline the pipeline with the processes to inject 
     * @param mainqueue the waiting queue
     * @param blocking does it block or fails if the queue is full.
     * @return true if event was injected in the pipeline.
     */
    public abstract boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue, boolean blocking);

    /**
     * This method inject a new event in a pipeline as
     * a top processing pipeline. Not to be used for sub-processing pipeline. It can only
     * be used for a new event.
     * <p>It will not block if the queue is full</p>
     * 
     * @param pipeline
     * @param mainqueue
     * @return true if event was injected in the pipeline.
     */
    public boolean inject(Pipeline pipeline, BlockingQueue<Event> mainqueue) {
        return inject(pipeline, mainqueue, false);
    }

    /**
     * Refill this event with the content of this pipeline. Used when forwarding an event 
     * to a another pipeline
     * 
     * @param pipeline
     */
    public abstract void refill(Pipeline pipeline);

    public abstract boolean inject(Event master, BlockingQueue<Event> mainqueue);

    public abstract void finishPipeline();

    public abstract Event duplicate();

    public abstract Processor next();

    public abstract void insertProcessor(Processor p);

    public abstract void appendProcessor(Processor p);

    public abstract void insertProcessors(List<Processor> p);

    public abstract void appendProcessors(List<Processor> p);

    public abstract String getCurrentPipeline();

    public abstract String getNextPipeline();

    public abstract boolean process(Processor p) throws ProcessorException;

    public abstract Date getTimestamp();

    public abstract void setTimestamp(Date timestamp);

    public abstract void end();

    public abstract int stepsCount();

    public abstract boolean isTest();

    public abstract void doMetric(Runnable metric);

    public abstract void drop();

    public abstract ConnectionContext<?> getConnectionContext();

    protected abstract EventInstance getRealEvent();

    public abstract Event unwrap();

    /**
     * An event is only equals to itself.
     * @param o object to be compared for equality with this map
     * @return <tt>true</tt> if the specified object is this event.
     */
    @Override
    public boolean equals(Object o) {
        return System.identityHashCode(this) == System.identityHashCode(o);
    }

    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

}
