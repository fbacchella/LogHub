package loghub.events;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

import loghub.ConnectionContext;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.UncheckedProcessorException;
import loghub.VariablePath;
import loghub.cloners.NotClonableException;
import loghub.metrics.Stats.PipelineStat;

@JsonIdentityInfo(generator = ObjectIdGenerators.IntSequenceGenerator.class)
public abstract class Event extends HashMap<String, Object> {

    public static final String TIMESTAMPKEY = "@timestamp";
    public static final String LASTEXCEPTIONKEY = "@lastException";
    public static final String CONTEXTKEY = "@context";
    public static final String INDIRECTMARK = "<-";
    public static final String EVENT_ENTRY = "loghub.Event";

    enum Action {
        APPEND(false, Action::append),          // Exported through appendAtPath
        GET(false, Action::get),       // Exported through getAtPath
        PUT(false, Action::put),  // Exported through putAtPath
        REMOVE(false, (i, j, k) -> i.remove(j)), // Exported through removeAtPath
        CONTAINS(true, (c, k, v) -> c.containsKey(k)), // Exported through containsAtPath
        CONTAINSVALUE(true, (c, k, v) -> Action.asMap(c, k).containsValue(v)), // Used in EventWrapper
        ISEMPTY(true, (c, k, v) -> Action.asMap(c, k).isEmpty()), // Used in EventWrapper
        SIZE(true, (c, k, v) ->  Action.size(c, k)), // Used in EventWrapper
        CLEAR(true, (c, k, v) -> { asMap(c, k).clear(); return null;}), // Used in EventWrapper
        KEYSET(true, (c, k, v) -> Action.asMap(c, k).keySet()), // Used in EventWrapper
        ENTRYSET(true, (c, k, v) -> Action.entrySet(Action.asMap(c, k))), // Used in EventWrapper
        VALUES(true, (c, k, v) -> Action.asMap(c, k).values()), // Used in EventWrapper
        CHECK_WRAP(true, Action::checkPath)
        ;
        private static Object get(Map<String, Object> c, String k, Object v) {
            Object value = c.get(k);
            assert value != NullOrMissingValue.MISSING;
            return value == NullOrMissingValue.NULL ? null : value;
        }
        private static Object getNotNull(Map<String, Object> m, String k) {
            Object o = m.get(k);
            assert o != NullOrMissingValue.MISSING;
            return o == null ? NullOrMissingValue.NULL : o;
        }
        private static Object put(Map<String, Object> c, String k, Object v) {
            assert v != NullOrMissingValue.MISSING;
            Object r = c.put(k, v == null ? NullOrMissingValue.NULL : v);
            return r == NullOrMissingValue.NULL ? null : r;
        }
        @SuppressWarnings("unchecked")
        private static Map<String, Object> asMap(Map<String, Object> c , String k) {
            if (k == null) {
                return c;
            } else if (c.containsKey(k) && c.get(k) instanceof Map) {
                return (Map<String, Object>) c.get(k);
            } else {
                return Collections.emptyMap();
            }
        }
        private static Set<Entry<String, Object>> entrySet(Map<String, Object> m) {
            return m.keySet().stream().map(k -> Map.entry(k, getNotNull(m, k))).collect(Collectors.toSet());
        }
        private static int size(Map<String, Object> c, String k) {
            if (k == null) {
                return c.size();
            } else if (c.containsKey(k) && c.get(k) instanceof Map) {
                return ((Map<?, ?>) c.get(k)).size();
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        private static Object checkPath(Map<String, Object> c, String k, Object v) {
            if (k != null && ! (c.get(k) instanceof Map)) {
                throw IgnoredEventException.INSTANCE;
            } else {
                return v;
            }
        }
        private static boolean append(Map<String, Object> c, String k, Object v) {
            if (k == null) {
                return false;
            } else if (c.containsKey(k)) {
                Object oldVal = c.get(k);
                if (oldVal == null || oldVal instanceof NullOrMissingValue) {
                    c.put(k, new ArrayList<>(List.of(v)));
                    return true;
                } else if (oldVal instanceof Collection) {
                    ((Collection<Object>) oldVal).add(v);
                    return true;
                } else if (oldVal instanceof char[] && v instanceof Character) {
                    char[] oldValArray = (char[]) oldVal;
                    char[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Character) v);
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof boolean[] && v instanceof Boolean) {
                    boolean[] oldValArray = (boolean[]) oldVal;
                    boolean[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Boolean) v);
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof byte[] && v instanceof Number) {
                    byte[] oldValArray = (byte[]) oldVal;
                    byte[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).byteValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof short[] && v instanceof Number) {
                    short[] oldValArray = (short[]) oldVal;
                    short[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).shortValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof int[] && v instanceof Number) {
                    int[] oldValArray = (int[]) oldVal;
                    int[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).intValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof long[] && v instanceof Number) {
                    long[] oldValArray = (long[]) oldVal;
                    long[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).longValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof float[] && v instanceof Number) {
                    float[] oldValArray = (float[]) oldVal;
                    float[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).floatValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal instanceof double[] && v instanceof Number) {
                    double[] oldValArray = (double[]) oldVal;
                    double[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = ((Number) v).doubleValue();
                    c.put(k, newVal);
                    return true;
                } else if (oldVal.getClass().isArray()
                        && (v == null
                            || v == NullOrMissingValue.NULL
                            || oldVal.getClass().getComponentType().isAssignableFrom(v.getClass()))) {
                    Object[] oldValArray = (Object[]) oldVal;
                    Object[] newVal = Arrays.copyOf(oldValArray, oldValArray.length + 1);
                    newVal[oldValArray.length] = (v == NullOrMissingValue.NULL) ? null : v;
                    c.put(k, newVal);
                    return true;
                } else {
                    return false;
                }
            } else if (v == null) {
                c.put(k, new ArrayList<>(List.of(NullOrMissingValue.NULL)));
                return true;
            } else {
                c.put(k, new ArrayList<>(List.of(v)));
                return true;
            }
        }
        public final Helpers.TriFunction<Map<String, Object>, String, Object, Object> action;
        public final boolean mapAction;
        Action(boolean mapAction, Helpers.TriFunction<Map<String, Object>, String, Object, Object> action) {
            this.mapAction = mapAction;
            this.action = action;
        }
    }

    private final Queue<Throwable> exceptionStack = Collections.asLifoQueue(new ArrayDeque<>());

    public Object applyAtPath(Action f, VariablePath path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    public Event wrap(VariablePath vp) {
        if (vp.isMeta() || vp.isTimestamp()) {
            throw new IllegalArgumentException("Wrap only on attributes path");
        }
        if (vp.isIndirect()) {
            vp = VariablePath.of(vp);
        }
        if (vp != VariablePath.EMPTY) {
            return new EventWrapper(this, vp);
        } else {
            return this;
        }
    }

    public abstract Event unwrap();

    @SuppressWarnings("unchecked")
    public Object applyAtPath(Action f, VariablePath path, Object value, boolean create) {
        if (value == NullOrMissingValue.MISSING) {
            switch (f) {
            case CONTAINS:
            case CONTAINSVALUE:
                return false;
            case PUT:
            case REMOVE:
            case APPEND:
                throw IgnoredEventException.INSTANCE;
            default:
                // skip
            }
        }
        if (path.isIndirect()) {
            Function<Object, VariablePath> convert = o -> {
                if (o instanceof String[]) {
                    return VariablePath.of((String[]) o);
                } else if (o.getClass().isArray() || o instanceof NullOrMissingValue) {
                    return null;
                } else {
                    return VariablePath.parse(o.toString());
                }
            };
            path = Optional.ofNullable(applyAtPath(Action.GET, VariablePath.of(path), false))
                           .map(convert)
                           .orElse(null);
            if (path == null) {
                return NullOrMissingValue.MISSING;
            }
        }
        if (path.isMeta()) {
            return applyMetaPath(f, path, value);
        } else if (path.isContext()) {
            return applyContextPath(f, path, value);
        } else if (path.isTimestamp()) {
            return applyTimestampPath(f, path, value);
        } else if (path.isException()) {
            return applyExceptionPath(f);
        } else if (path == VariablePath.EMPTY) {
            return applyEmptyPath(f, value);
        } else if (path == VariablePath.ROOT) {
            return applyRelativePath(getRealEvent(), f, value);
        } else if (path == VariablePath.CURRENT) {
            return applyCurrentPath(f, value);
        } else {
            return applyPlainPath(f, path, value, create);
        }
    }

    private Object applyMetaPath(Action f, VariablePath path, Object value) {
        if (path.length() != 0) {
            return switch (f) {
                case GET -> getMeta(path.get(0));
                default -> f.action.apply(getMetas(), path.get(0), value);
            };
        } else if (f.mapAction) {
            return f.action.apply(getMetas(), null, value);
        } else {
            throw new IllegalArgumentException("No variable specified for " + f);
        }
    }

    private Object applyTimestampPath(Action f, VariablePath path, Object value) {
        return switch (f) {
            case GET -> getTimestamp();
            case PUT -> {
                Date oldTimestamp = getTimestamp();
                if (!setTimestamp(value)) {
                    throw new IllegalArgumentException(value + " is not usable as a timestamp from path " + path);
                }
                yield oldTimestamp;
            }
            default -> throw new IllegalArgumentException("Invalid action on a timestamp");
        };
    }

    private Object applyExceptionPath(Action f) {
        if (f == Action.GET) {
            return Optional.ofNullable(getLastException())
                           .map(Helpers::resolveThrowableException)
                           .map(Object.class::cast)
                           .orElse(NullOrMissingValue.MISSING);
        } else {
            throw new IllegalArgumentException("Invalid action on a last exception");
        }
    }

    private Object applyEmptyPath(Action f, Object value) {
        if (f.mapAction) {
            return f.action.apply(this, null, value);
        } else {
            throw new IllegalArgumentException("No variable specified for " + f);
        }
    }

    private Object applyCurrentPath(Action f, Object value) {
        if (!containsData()) {
            throw IgnoredEventException.INSTANCE;
        }
        return applyRelativePath(this, f, value);
    }

    private Object applyPlainPath(Action f, VariablePath path, Object value, boolean create) {
        Map<String, Object> current = this;
        String key = path.get(0);
        for (int i = 0; i < path.length() - 1; i++) {
            String currentkey = path.get(i);
            if (".".equals(currentkey)) {
                current = getRealEvent();
            } else {
                Optional<Object> peekNext = Optional.of(current)
                                                    .filter(c -> c.containsKey(currentkey))
                                                    .map(c -> c.get(currentkey));
                Map<String, Object> next;
                if (peekNext.isEmpty()) {
                    if (create) {
                        next = new HashMap<>();
                        current.put(path.get(i), next);
                    } else {
                        return keyMissing(f);
                    }
                } else if (!(peekNext.get() instanceof Map) && f == Action.CHECK_WRAP) {
                    throw IgnoredEventException.INSTANCE;
                } else if (!(peekNext.get() instanceof Map)) {
                    return keyMissing(f);
                } else {
                    next = (Map<String, Object>) peekNext.get();
                }
                current = next;
            }
            key = path.get(i + 1);
        }
        if (create && !current.containsKey(key) && f != Action.APPEND) {
            current.put(key, new HashMap<>());
        } else if (!current.containsKey(key) && f != Action.PUT && f != Action.APPEND) {
            return keyMissing(f);
        }
        return f.action.apply(current, key, value);
    }

    private Object applyContextPath(Action f, VariablePath path, Object value) {
        if (this.getConnectionContext() instanceof LockedConnectionContext lcc) {
            return switch (f) {
                case GET -> VariablePath.resolveContext(this, path);
                case CONTAINS -> VariablePath.resolveContext(this, path) != null;
                case CONTAINSVALUE -> lcc.containsValue(value);
                case ISEMPTY -> false;
                case SIZE -> lcc.size();
                case KEYSET -> lcc.getProperties().keySet();
                case VALUES -> lcc.getProperties().values();
                case ENTRYSET -> lcc.getProperties().entrySet();
                default -> throw new IllegalArgumentException("Invalid action on context: " + f);
            };
        } else {
            throw new IllegalArgumentException("Not a valid context");
        }
    }

    private Object applyRelativePath(Event ev, Action f, Object value) {
        return switch (f) {
            case GET -> ev;
            case CONTAINS -> true;
            case REMOVE -> {
                Object oldValue = Expression.deepCopy(ev);
                Action.CLEAR.action.apply(ev, null, value == NullOrMissingValue.NULL ? null : value);
                yield oldValue;
            }
            case PUT -> {
                if (value instanceof Map<?, ?> m) {
                    m.entrySet().forEach(e -> ev.put(e.getKey().toString(), Expression.deepCopy(e.getValue())));
                    yield ev;
                } else {
                    throw IgnoredEventException.INSTANCE;
                }
            }
            default -> f.action.apply(ev, null, value == NullOrMissingValue.NULL ? null : value);
        };
    }

    private Object keyMissing(Action f) {
        return switch (f) {
            case GET, CHECK_WRAP -> NullOrMissingValue.MISSING;
            case CONTAINSVALUE, CONTAINS -> false;
            case KEYSET, VALUES, ENTRYSET -> Set.of();
            default -> throw IgnoredEventException.INSTANCE;
        };
    }

    public void clearMetas() {
        getMetas().clear();
    }

    public abstract Map<String, Object> getMetas();

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

    public Object getGroovyLastException() {
        return Optional.ofNullable(getLastException())
                       .map(Helpers::resolveThrowableException)
                       .map(Object.class::cast)
                       .orElse(NullOrMissingValue.MISSING);
    }

    /**
     * Used in groovy code only
     * @param vpid a {@link VariablePath} unique identifier
     * @return the value of event at {@link VariablePath} matching vpid
     */
    public Object getGroovyPath(int vpid) {
        VariablePath vp = VariablePath.getById(vpid);
        return Optional.ofNullable(applyAtPath(Action.GET, vp, null, false)).orElse(NullOrMissingValue.NULL);
    }

    /**
     * This method inject a new event in the processing pipeline. Used by receivers when creating new events.
     *
     * @param pipeline the pipeline with the processes to inject
     * @param mainqueue the waiting queue
     * @param blocking does it block or fails if the queue is full.
     * @return true if event was injected in the pipeline.
     */
    public abstract boolean inject(Pipeline pipeline, PriorityBlockingQueue mainqueue, boolean blocking);

    /**
     * This method inject a new event in the processing pipeline. Used by processors that want to inject new events.
     * <p>It will not block if the queue is full.</p>
     *
     * @param pipeline
     * @param mainqueue
     */
    public abstract void reinject(Pipeline pipeline, PriorityBlockingQueue mainqueue);

    /**
     * Inject this event in the processing pipelines, using another event as a reference for the state.
     * <p>It will not block if the queue is full.</p>
     * @param reference
     * @param mainqueue
     */
    public abstract void reinject(Event reference, PriorityBlockingQueue mainqueue);

    /**
     * Refill this event with the content of this pipeline. Used when forwarding an event
     * to a another pipeline
     *
     * @param pipeline
     */
    public abstract void refill(Pipeline pipeline);

    public abstract void finishPipeline();

    public abstract Event duplicate() throws NotClonableException;

    public abstract Processor next();

    public abstract void insertProcessor(Processor p);

    public abstract void appendProcessor(Processor p);

    public abstract void insertProcessors(List<Processor> p);

    public abstract void appendProcessors(List<Processor> p);

    public abstract String getCurrentPipeline();

    public abstract String getNextPipeline();

    public abstract String getRunningPipeline();

    public abstract boolean process(Processor p) throws ProcessorException;

    public abstract Date getTimestamp();

    public abstract void setTimestamp(Date timestamp);

    public void setTimestamp(Instant instant) {
        setTimestamp(Date.from(instant));
    }

    public boolean setTimestamp(Object value) {
        switch (value) {
        case IgnoredEventException iee -> {
            return false;
        }
        case Date d -> {
            setTimestamp(d);
            return true;
        }
        case Instant i -> {
            setTimestamp(Date.from(i));
            return true;
        }
        case ZonedDateTime zdt -> {
            setTimestamp(Date.from(zdt.toInstant()));
            return true;
        }
        case LocalDateTime ldt -> {
            setTimestamp(Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant()));
            return true;
        }
        case Number n -> {
            setTimestamp(new Date(n.longValue()));
            return true;
        }
        case null, default -> {
            return false;
        }
        }
    }

    public boolean appendAtPath(VariablePath path, Object o) {
        Boolean status = (Boolean) applyAtPath(Action.APPEND, path, o, true);
        if (status == null) {
            throw IgnoredEventException.INSTANCE;
        } else {
            return status;
        }
    }

    public Object putAtPath(VariablePath path, Object o) {
        return applyAtPath(Action.PUT, path, o, true);
    }

    public Object getAtPath(VariablePath path) {
        return applyAtPath(Action.GET, path, null, false);
    }

    public boolean containsAtPath(VariablePath path) {
        return Boolean.TRUE.equals(applyAtPath(Action.CONTAINS, path, null, false));
    }

    public abstract boolean containsData();

    public Object removeAtPath(VariablePath path) {
        return applyAtPath(Action.REMOVE, path, null, false);
    }

    /**
     * Recursively extracts all the {@link VariablePath} for each non-map value found in the event.
     * <p>
     * It performs a depth-first traversal of the event.
     * <p>
     * Paths for values equal to {@link NullOrMissingValue#MISSING} are ignored and not returned.
     * Nested maps are traversed to build full paths to the leaves.
     *
     * @return a {@link Stream} of {@link VariablePath} representing all found keys and subkeys
     */
    public Stream<VariablePath> enumerateAllPaths() {
        return enumerateAllPaths(this, VariablePath.EMPTY);
    }

    private Stream<VariablePath> enumerateAllPaths(Map<String, Object> map, VariablePath parent) {
        return map.entrySet()
                  .stream()
                  .flatMap(entry -> recursePaths(parent, entry.getKey(), entry.getValue()));
    }

    @SuppressWarnings("unchecked")
    private Stream<VariablePath> recursePaths(VariablePath parent, String key, Object value) {
        VariablePath currentPath = parent.append(key);
        if (value instanceof Map<?, ?> nestedMap) {
            return enumerateAllPaths((Map<String, Object>) nestedMap, currentPath);
        } else if (value == NullOrMissingValue.MISSING) {
            return Stream.empty();
        } else {
            return Stream.of(currentPath);
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

    public abstract Logger getPipelineLogger();

    public void pushException(Throwable t) {
        exceptionStack.add(t);
    }

    public Throwable popException() {
        return exceptionStack.poll();
    }
    public Throwable getLastException() {
        return exceptionStack.peek();
    }

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

    abstract Event deepClone() throws NotClonableException;
}
