package loghub;

import java.io.Serializable;
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

    public static Event emptyEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx);
    }

    public static Event emptyTestEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx, true);
    }

    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    @SuppressWarnings("unchecked")
    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value, boolean create) {
        Map<String, Object> current = this;
        String key = path[0];
        if (key != null && key.startsWith("#")) {
            return f.apply(getMetas(), key.substring(1), value);
        } else {
            for (int i = 0; i < path.length - 1; i++) {
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
            return f.apply(current, key, value);
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
