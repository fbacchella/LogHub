package loghub.processors;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import loghub.AsyncProcessor;
import loghub.BuilderClass;
import loghub.DiscardedEventException;
import loghub.EventsRepository;
import loghub.Expression;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(Merge.Builder.class)
public class Merge extends Processor {

    public static class Builder extends Processor.Builder<Merge> {
        @Setter
        private Expression index;
        @Setter
        private Expression doFire = null;
        @Setter
        private Object defaultSeed = new Object[]{};
        @Setter
        private Map<Object, Object> seeds = Collections.emptyMap();
        @Setter
        private Processor expirationProcessor = new Identity();
        @Setter
        private Processor onFire = new Identity();
        @Setter
        private int expiration = Integer.MAX_VALUE;
        @Setter
        private boolean forward = false;
        @Setter
        private VariablePath durationField = VariablePath.ofMeta("duration");

        public void setDefault(Object defaultSeed) {
            this.defaultSeed = defaultSeed;
        }

        public Merge build() {
            return new Merge(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private enum Cumulator {
        STRING {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                String stringSeed = seed == null ? "" : seed.toString();
                return (last, next) -> last == null ? String.valueOf(next) : last + stringSeed + next;
            }
        },
        LIST {
            @SuppressWarnings("unchecked")
            private List<Object> object2list(Object source) {
                List<Object> newList;
                if (source == null) {
                    return new ArrayList<>();
                } else if (source instanceof Collection<?> c) {
                    return new ArrayList<>(c);
                } else if (source.getClass().isArray()) {
                    int length = Array.getLength(source);
                    newList = new ArrayList<>(length);
                    for (int i = 0 ; i < length ; i++) {
                        newList.add(Array.get(source, i));
                    }
                } else {
                    newList = new ArrayList<>();
                    newList.add(source);
                }
                return newList;
            }
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                List<Object> listSeed = object2list(seed);
                return (last, next) -> {
                    List<Object> newList = object2list(last);
                    if (last == null) {
                        newList.addAll(listSeed);
                    }
                    newList.addAll(object2list(next));
                    return newList;
                };
            }
        },
        SET {
            @SuppressWarnings("unchecked")
            private Set<Object> object2set(Object source) {
                Set<Object> newList;
                if (source == null || source instanceof NullOrMissingValue) {
                    return new LinkedHashSet<>();
                } else if (source instanceof Collection<?> c) {
                    return new LinkedHashSet<>(c);
                } else if (source.getClass().isArray()) {
                    int length = Array.getLength(source);
                    newList = LinkedHashSet.newLinkedHashSet(length);
                    for (int i = 0 ; i < length ; i++) {
                        newList.add(Array.get(source, i));
                    }
                } else {
                    newList = new LinkedHashSet<>();
                    newList.add(source);
                }
                return newList;
            }
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                Set<Object> listSeed = object2set(seed);
                return (last, next) -> {
                    Set<Object> newList = object2set(last);
                    if (last == null) {
                        newList.addAll(listSeed);
                    }
                    newList.addAll(object2set(next));
                    return newList;
                };
            }
        },
        MAP {
            // This method can return the source unmodified
            // Modifing the returned object must be done with care
            @SuppressWarnings("unchecked")
            private Map<Object, Object> object2Map(Object source) {
                if (source == null) {
                    return new HashMap<>();
                } else if (source instanceof Map) {
                    return (Map<Object, Object>) source;
                } else {
                    // Can't fill a map with a single value, a key is needed
                    return new HashMap<>();
                }
            }
            @SuppressWarnings("unchecked")
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                final Map<Object, Object> mapseed = object2Map(seed);
                return (last, next) -> {
                    Map<Object, Object> newmap = object2Map(last);
                    if (last == null) {
                        newmap.putAll(mapseed);
                    }
                    object2Map(next).forEach((k, v) -> {
                        if (newmap.containsKey(k)) {
                            Object oldValue = newmap.get(k);
                            if (oldValue instanceof List) {
                                ((List<Object>) oldValue).add(v);
                            } else if (oldValue instanceof Map && v instanceof Map) {
                                object2Map(oldValue).putAll(object2Map(v));
                            } else {
                                List<Object> newValue = new ArrayList<>();
                                newValue.add(oldValue);
                                newValue.add(v);
                                newmap.put(k, newValue);
                            }
                        } else {
                            newmap.put(k, v);
                        }
                    });
                    return newmap;
                };
            }
        },
        AND {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> last == null ? toBoolean(next) : Boolean.logicalAnd((boolean) last, toBoolean(next));
            }
        },
        OR {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> last == null ? toBoolean(next) :  Boolean.logicalOr((boolean) last, toBoolean(next));
            }
        },
        ADD {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long lnext = toLong(next);
                    Long llast = toLong(last);
                    return lnext != null && llast != null ? llast + lnext :
                        llast == null ? lnext : llast;
                };
            }
        },
        MULTIPLY {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long lnext = toLong(next);
                    Long llast = toLong(last);
                    return lnext != null && llast != null ? llast * lnext :
                        llast == null ? lnext : llast;
                };
            }
        },
        ADDFLOAT {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    Double dnext = toDouble(next);
                    Double dlast = toDouble(last);
                    return dnext != null && dlast != null ? dlast + dnext :
                        dlast == null ? dnext : dlast;
                };
            }
        },
        MULTIPLYFLOAT {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    Double dnext = toDouble(next);
                    Double dlast = toDouble(last);
                    return dnext != null && dlast != null ? dlast * dnext :
                        dlast == null ? dnext : dlast;
                };
            }
        },
        LAST {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> next != null ? next : last;
            }
        },
        FIRST {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> last != null ? last : next;
            }
        },
        DROP {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> null;
            }
        },
        COUNT {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long llast = toLong(last);
                    return llast != null ? llast + 1 : 1;
                };
            }
        },
        DEFAULT {
            @Override
            BinaryOperator<Object> cumulate(Object seed) {
                return (last, next) -> {
                    if (next == null || next instanceof NullOrMissingValue) {
                        return last;
                    } else if (last == null || last instanceof NullOrMissingValue) {
                        return next;
                    } else if (last instanceof String) {
                        return new StringBuilder(last.toString()).append(next);
                    } else if (last instanceof StringBuilder sb) {
                        return sb.append(next);
                    } else if (last instanceof Number nl && next instanceof Number nn) {
                        return nl.longValue() + nn.longValue();
                    } else if (last instanceof Date || next instanceof Date) {
                        return new Date();
                    } else {
                        return next;
                    }
                };
            }
        },
        ;

        abstract BinaryOperator<Object> cumulate(Object seed);
        private static boolean toBoolean(Object o) {
            if (o == null || o == NullOrMissingValue.NULL) {
                return false;
            } else if (o instanceof Boolean b) {
                return b;
            } else if (o instanceof String) {
                return Boolean.parseBoolean(o.toString());
            } else if (o instanceof Integer || o instanceof Long) {
                return ((Number) o).intValue() != 0;
            } else if (o instanceof Number n) {
                return n.doubleValue() != 0;
            } else {
                return false;
            }
        }
        private static Long toLong(Object o) {
            if (o == null || o == NullOrMissingValue.NULL) {
                return null;
            } else if (o instanceof Long l) {
                return l;
            } else if (o instanceof Number n) {
                return n.longValue();
            } else if (o instanceof Boolean) {
                return Boolean.TRUE.equals(o) ? 1L : 0L;
            } else if (o instanceof String) {
                try {
                    return Long.parseLong(o.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        private static Double toDouble(Object o) {
            if (o == null || o == NullOrMissingValue.NULL) {
                return null;
            } else if (o instanceof Double d) {
                return d;
            } else if (o instanceof Number n) {
                return n.doubleValue();
            } else if (o instanceof Boolean) {
                return Boolean.TRUE.equals(o) ? 1.0 : 0.0;
            } else if (o instanceof String) {
                try {
                    return Double.parseDouble(o.toString());
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                return null;
            }
        }
        static BinaryOperator<Object> getCumulator(Object o) {
            if (o == null || o instanceof NullOrMissingValue) {
                return Cumulator.DROP.cumulate(o);
            } else if (o instanceof String) {
                return Cumulator.STRING.cumulate(o);
            } else if (o instanceof Character c) {
                return switch (c) {
                    case '<' -> Cumulator.FIRST.cumulate(o);
                    case '>' -> Cumulator.LAST.cumulate(o);
                    case 'c' -> Cumulator.COUNT.cumulate(o);
                    default -> Cumulator.LIST.cumulate(o);
                };
            } else if (o instanceof Boolean && Boolean.TRUE.equals(o)) {
                return Cumulator.AND.cumulate(o);
            } else if (o instanceof Boolean b && Boolean.TRUE.equals(! b)) {
                return Cumulator.OR.cumulate(o);
            } else if ((o instanceof Integer || o instanceof Long) && ((Number) o).longValue() == 0) {
                return Cumulator.ADD.cumulate(o);
            } else if ((o instanceof Integer || o instanceof Long) && ((Number) o).longValue() == 1) {
                return Cumulator.MULTIPLY.cumulate(o);
            } else if ((o instanceof Float || o instanceof Double) && ((Number) o).doubleValue() == 0) {
                return Cumulator.ADDFLOAT.cumulate(o);
            } else if ((o instanceof Float || o instanceof Double) && ((Number) o).longValue() == 1) {
                return Cumulator.MULTIPLYFLOAT.cumulate(o);
            } else if (o instanceof List || o.getClass().isArray()) {
                return Cumulator.LIST.cumulate(o);
            } else if (o instanceof Set) {
                return Cumulator.SET.cumulate(o);
            } else if (o instanceof Map) {
                return Cumulator.MAP.cumulate(o);
            } else if (o instanceof Expression ex) {
                try {
                    return getCumulator(ex.eval());
                } catch (ProcessorException e) {
                    throw new IllegalArgumentException("Can't identify the merge format: " + Helpers.resolveThrowableException(e), e);
                }
            } else {
                return Cumulator.LIST.cumulate(o);
            }
        }
    }

    private static final UnaryOperator<Event> prepareEvent = i -> {
        i.forEach((key, value) -> {
            if (value instanceof StringBuilder)
                i.put(key, value.toString());
        });
        return i;
    };

    private final Expression index;
    private final Expression fire;
    private final BinaryOperator<Object> defaultCumulator;
    private final Map<VariablePath, Object> seeds;
    private final Map<VariablePath, BinaryOperator<Object>> cumulators;
    private EventsRepository<Object> repository = null;
    private final Processor expirationProcessor;
    private final Processor fireProcessor;
    private final int expiration;
    private final boolean forward;
    private final VariablePath durationField;

    public Merge(Builder builder) {
        super(builder);
        this.index = builder.index;
        if (index == null) {
            throw new IllegalArgumentException("Index setting is mandatory");
        }

        this.fire = builder.doFire;
        this.defaultCumulator = Cumulator.getCumulator(builder.defaultSeed);
        this.seeds = builder.seeds.entrySet()
                                  .stream()
                                  .collect(Collectors.toMap(
                                          e -> convertVariablePath(e.getKey()),
                                          Entry::getValue)
                                  );
        cumulators = new ConcurrentHashMap<>(seeds.size() + 1);
        this.expirationProcessor = builder.expirationProcessor;
        this.fireProcessor = builder.onFire;
        this.expiration = builder.expiration;
        this.forward = builder.forward;
        // Default to timestamp is to keep the first
        cumulators.put(VariablePath.TIMESTAMP, Cumulator.FIRST.cumulate(null));
        for (Entry<VariablePath, Object> i : seeds.entrySet()) {
            cumulators.put(i.getKey(), Cumulator.getCumulator(i.getValue()));
        }
        this.durationField = builder.durationField;
    }

    private VariablePath convertVariablePath(Object o) {
        if (o instanceof VariablePath vp) {
            return vp;
        } else {
            return VariablePath.parse(o.toString());
        }
    }

    @Override
    public boolean configure(Properties properties) {
        repository = new EventsRepository<>(properties);
        // Prepare fire only if test and processor given for that
        if (fire != null && fireProcessor != null && ! fireProcessor.configure(properties)) {
            return false;
        }
        if (expirationProcessor != null && ! expirationProcessor.configure(properties)) {
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        Object eventKey;
        try {
            eventKey = index.eval(event);
        } catch (ProcessorException e) {
            // Expression failed, not to be merged
            return false;
        }
        // If the key is null, can't use the event
        if (eventKey == null) {
            throw IgnoredEventException.INSTANCE;
        }
        logger.trace("key: {} for {}", eventKey, event);
        PausedEvent<Object> current = repository.getOrPause(eventKey, i -> getPausedEvent(event, i));
        // If we didn't get back the same event, we are actually merging the event.
        if (event != current.event) {
            synchronized (current) {
                logger.trace("merging {} in {}", event, current.event);
                // Ensure that the timestamp is not forgotten
                Date lastTimestamp = current.event.getTimestamp();
                Date nextTimestamp = event.getTimestamp();
                Object newTimestamp = cumulators.getOrDefault(VariablePath.TIMESTAMP, (o1, o2) -> o1).apply(lastTimestamp, nextTimestamp);
                current.event.setTimestamp(newTimestamp);
                current.event.putAtPath(durationField, Duration.ofMillis(nextTimestamp.getTime() - lastTimestamp.getTime()));
                event.enumerateAllPaths()
                     .forEach(vp -> doCumulate(current, event, vp, true));
                event.getMetaAsStream()
                     .map(e -> VariablePath.ofMeta(e.getKey()))
                     .forEach(vp -> doCumulate(current, event, vp, false));
                if (fire != null) {
                    Object dofire = fire.eval(current.event);
                    if (Boolean.TRUE.equals(dofire)) {
                        logger.trace("fire {}", eventKey);
                        repository.succed(eventKey);
                    }
                }
            }
            if (! forward) {
                throw DiscardedEventException.INSTANCE;
            } else {
                return true;
            }
        } else {
            logger.trace("pausing {} in {}", event, current.event);
            throw new AsyncProcessor.PausedEventException();
        }
    }

    private void doCumulate(PausedEvent<Object> current, Event ev, VariablePath vp, boolean useDefault) {
        Object last = Optional.ofNullable(current.event.getAtPath(vp)).filter(o -> ! (o instanceof NullOrMissingValue)).orElse(null);
        Object next = Optional.ofNullable(ev.getAtPath(vp)).filter(o -> ! (o instanceof NullOrMissingValue)).orElse(null);
        if (useDefault || cumulators.containsKey(vp)) {
            BinaryOperator<Object> m = cumulators.getOrDefault(vp, defaultCumulator);
            Object newValue = m.apply(last, next);
            if (newValue != null && ! (newValue instanceof NullOrMissingValue)) {
                current.event.putAtPath(vp, newValue);
            }
        }
    }

    private PausedEvent<Object> getPausedEvent(Event event, Object key) {
        PausedEvent.Builder<Object> builder = PausedEvent.builder(event, key);
        return builder.expiration(expiration, TimeUnit.SECONDS).onExpiration(expirationProcessor, prepareEvent)
                      .onSuccess(fireProcessor, prepareEvent)
                      .build();
    }

    @Override
    public String getName() {
        return "Merge/" + index.getSource();
    }

    public Processor getOnExpire() {
        return expirationProcessor;
    }

    public Processor getOnFire() {
        return fireProcessor;
    }

    public Expression getDoFire() {
        return fire;
    }

    /**
     * @return the expiration duration
     */
    public Integer getExpiration() {
        return expiration;
    }

    /**
     * @return the forward
     */
    public Boolean isForward() {
        return forward;
    }

}
