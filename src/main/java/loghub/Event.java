package loghub;

import java.io.Serializable;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import loghub.metrics.Stats.PipelineStat;

@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
public abstract class Event extends HashMap<String, Object> implements Serializable {

    public static final String TIMESTAMPKEY = "@timestamp";
    public static final String CONTEXTKEY = "@context";
    public static final String INDIRECTMARK = "<-";

    public enum Action {
        GET(false, (i,j,k) -> i.get(j)),
        PUT(false, (i, j, k) -> i.put(j, k)),
        REMOVE(false, (i, j, k) -> i.remove(j)),
        CONTAINS(false, (i, j, k) -> i.containsKey(j)),
        CONTAINSVALUE(true, (c, k, v) -> Action.asMap(c, k).containsValue(v)),
        SIZE(true, (c, k, v) -> Action.asMap(c, k).size()),
        ISEMPTY(true, (c, k, v) -> Action.asMap(c, k).isEmpty()),
        CLEAR(true, (c, k, v) -> {asMap(c, k).clear(); return null;}),
        KEYSET(true, (c, k, v) -> Action.asMap(c, k).keySet()),
        VALUES(true, (c, k, v) -> Action.asMap(c, k).values()),
        ;
        @SuppressWarnings("unchecked")
        private static Map<String, Object> asMap(Map<String, Object>c , String k) {
            if (k == null) {
                return c;
            } else if (c.containsKey(k) && c.get(k) instanceof Map){
                return (Map<String, Object>)c.get(k);
            } else {
                return Collections.emptyMap();
            }
        }
        public final Helpers.TriFunction<Map<String, Object>, String, Object, Object> action;
        public final boolean mapAction;
        Action (boolean mapAction, Helpers.TriFunction<Map<String, Object>, String, Object, Object> action){
            this.mapAction = mapAction;
            this.action = action;
        }
    }

    public static Event emptyEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx);
    }

    public static Event emptyTestEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx, true);
    }

    public Object applyAtPath(Action f, VariablePath path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    @SuppressWarnings("unchecked")
    public Object applyAtPath(Action f, VariablePath path, Object value, boolean create) {
        if (path.isIndirect()) {
            Function<Object, String[]> convert = o -> {
                if (o instanceof String[]) {
                    return (String[]) o;
                } else if (o.getClass().isArray()) {
                    return new String[] {};
                } else {
                    return new String[] {o.toString()};
                }
            };
            path = Optional.ofNullable(applyAtPath(Action.GET, VariablePath.of(path), false))
                    .map(convert)
                    .map(VariablePath::of)
                    .orElse(VariablePath.EMPTY);
            if (path == VariablePath.EMPTY) {
                return null;
            }
        }
        Map<String, Object> current = this;
        String key = null;
        if (path.isMeta()) {
            return f.action.apply(getMetas(), path.get(0), value);
        } else if (path.isTimestamp()) {
            switch(f) {
            case GET: return getTimestamp();
            case PUT: {
                if (!setTimestamp(value)) {
                    throw new IllegalArgumentException(String.valueOf(value) + " is not usable as a timestamp from path " + path);
                };
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
        } else if (path == VariablePath.EMPTY) {
            switch(f) {
            case GET:
            case PUT:
            case REMOVE:
            case CONTAINS:
                throw new IllegalArgumentException("No variable specifed for " + f);
            case SIZE:
            case ISEMPTY:
            case CLEAR:
            case CONTAINSVALUE:
            case KEYSET:
            case VALUES:
                return f.action.apply(this, null, null);
            default: return null;
            }
        } else {
            key = path.get(0);
            for (int i = 0 ; i < path.length() - 1; i++) {
                String currentkey = path.get(i);
                if (".".equals(currentkey)) {
                    current = getRealEvent();
                    key = path.get(i + 1);
                } else {
                    Optional<Object> peekNext = Optional.of(current).filter(c -> c.containsKey(currentkey)).map(c -> c.get(currentkey));
                    Map<String, Object> next;
                    if (! peekNext.isPresent()) {
                        if (create) {
                            next = new HashMap<>();
                            current.put(path.get(i), next);
                        } else {
                            switch(f) {
                            case GET:
                            case SIZE:
                                throw IgnoredEventException.INSTANCE;
                            case CONTAINSVALUE:
                            case CONTAINS:
                                return false;
                            case ISEMPTY:
                                return true;
                            case KEYSET:
                                return Collections.emptySet();
                            case VALUES:
                                return Collections.emptySet();
                            default:
                                return null;
                            }
                        }
                    } else if (! (peekNext.get() instanceof Map)) {
                        throw new IllegalArgumentException("Can descend into " + key + " from " + path + " , it's not an object");
                    } else {
                        next = (Map<String, Object>) peekNext.get();
                    }
                    current = next;
                }
                key = path.get(i + 1);
            }
            if (create && f.mapAction && ! current.containsKey(key)) {
                current.put(key, new HashMap<>());
            }
            return f.action.apply(current, key, value);
        }
    }

    public void clearMetas() {
        getMetas().clear();
    }

    abstract Map<String, Object> getMetas();

    public abstract Object getMeta(String key);

    public abstract Object putMeta(String key, Object value);

    public abstract void mergeMeta(Event event, BiFunction<Object, Object, Object> cumulator);

    public abstract Stream<Entry<String, Object>> getMetaAsStream();

    public ProcessorException buildException(String message) {
        return new ProcessorException(getRealEvent(), message);
    }

    public ProcessorException buildException(String message, Exception root) {
        return new ProcessorException(getRealEvent(), message, root);
    }

    public UncheckedProcessorException wrapException(String message, Exception root) {
        return new UncheckedProcessorException(new ProcessorException(getRealEvent(), message, root));
    }

    public UncheckedProcessorException wrapException(String message) {
        return new UncheckedProcessorException(new ProcessorException(getRealEvent(), message));
    }

    
    /**
     * Used in groovy code
     * @param path
     * @return
     * @throws ProcessorException
     */
    public Object getPath(String...path) throws ProcessorException {
        return applyAtPath(Action.GET, VariablePath.of(path), null, false);
    }

    /**
     * Used in groovy code
     * @param path
     * @return
     * @throws ProcessorException
     */
    public Object getIndirectPath(String...path) throws ProcessorException {
        return applyAtPath(Action.GET, VariablePath.ofIndirect(path), null, false);
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
    public abstract boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking);

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
    public boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue) {
        return inject(pipeline, mainqueue, false);
    }

    /**
     * Refill this event with the content of this pipeline. Used when forwarding an event 
     * to a another pipeline
     * 
     * @param pipeline
     */
    public abstract void refill(Pipeline pipeline);

    /**
     * Inject this event in the processing pipelines, using another event as a reference for the state.
     * @param master
     * @param mainqueue
     * @return
     */
    public abstract boolean inject(Event master, PriorityBlockingQueue mainqueue);

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

    public void setTimestamp(Instant instant) {
        setTimestamp(Date.from(instant));
    }

    public boolean setTimestamp(Object value) {
        if (value instanceof Date) {
            setTimestamp((Date) value);
            return true;
        } else if (value instanceof Instant){
            setTimestamp(Date.from((Instant)value));
            return true;
        } else if (value instanceof Number){
            Date newDate = new Date(((Number)value).longValue());
            setTimestamp(newDate);
            return true;
        } else {
            return false;
        }
    }

    public abstract void end();

    public abstract int processingDone();

    public abstract int processingLeft();

    public abstract boolean isTest();

    public void doMetric(PipelineStat status) {
        doMetric(status, null);
    }

    public abstract void doMetric(PipelineStat status, Throwable ex);

    public abstract void drop();

    public abstract <T> ConnectionContext<T> getConnectionContext();

    protected abstract EventInstance getRealEvent();

    public abstract Event unwrap();

    /**
     * An event is only equals to itself.
     * @param o object to be compared for equality with this map
     * @return <code>true</code> if the specified object is this event.
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
