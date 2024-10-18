package loghub.processors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import loghub.AsyncProcessor;
import loghub.DiscardedEventException;
import loghub.EventsRepository;
import loghub.Expression;
import loghub.NullOrMissingValue;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

public class Merge extends Processor {

    private enum Cumulator {
        STRING {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                String stringSeed = seed == null ? "" : seed.toString();
                return (last, next) -> last == null ? String.valueOf(next) : last + stringSeed + next;
            }
        },
        LIST {
            @SuppressWarnings("unchecked")
            // This method can return the source unmodified
            // Modifing the returned object must be done with care
            private List<Object> object2list(Object source) {
                List<Object> newList;
                if (source == null) {
                    return new ArrayList<>();
                } else if (source instanceof List) {
                    return (List<Object>) source;
                } else if (source.getClass().isArray()) {
                    Object[] seedArray = (Object[]) source;
                    newList = new ArrayList<>(Arrays.asList(seedArray));
                } else {
                    newList = new ArrayList<>();
                    newList.add(source);
                }
                return newList;
            }
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
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
            BiFunction<Object, Object, Object> cumulate(Object seed) {
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
                            } else if (oldValue instanceof Map && v instanceof Map){
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
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> last == null ? toBoolean(next) : Boolean.logicalAnd((boolean) last, toBoolean(next));
            }
        },
        OR {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> last == null ? toBoolean(next) :  Boolean.logicalOr((boolean) last, toBoolean(next));
            }
        },
        ADD {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long lnext = toLong(next) ; Long llast = toLong(last);
                    return lnext != null && llast != null ? llast + lnext :
                        llast == null ? lnext : llast;
                };
            }
        },
        MULTIPLY {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long lnext = toLong(next) ; Long llast = toLong(last);
                    return lnext != null && llast != null ? llast * lnext :
                        llast == null ? lnext : llast;
                };
            }
        },
        ADDFLOAT {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    Double dnext = toDouble(next) ; Double dlast = toDouble(last);
                    return dnext != null && dlast != null ? dlast + dnext :
                        dlast == null ? dnext : dlast;
                };
            }
        },
        MULTIPLYFLOAT {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    Double dnext = toDouble(next) ; Double dlast = toDouble(last);
                    return dnext != null && dlast != null ? dlast * dnext :
                        dlast == null ? dnext : dlast;
                };
            }
        },
        LAST {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> next != null ? next : last;
            }
        },
        FIRST {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> last != null ? last : next;
            }
        },
        DROP {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> null;
            }
        },
        COUNT {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    Long llast = toLong(last);
                    return llast != null ? llast + 1 : 1;
                };
            }
        },
        DEFAULT {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> {
                    if (next == null) {
                        return last;
                    } else if (last == null) {
                        return next;
                    } else if (last instanceof String) {
                        return new StringBuilder(last.toString()).append(next);
                    } else if (last instanceof StringBuilder) {
                        return ((StringBuilder)last).append(next);
                    } else if (last instanceof Number && next instanceof Number) {
                        return ((Number)last).longValue() + ((Number)next).longValue();
                    } else if (last instanceof Date || next instanceof Date) {
                        return new Date();
                    } else {
                        return next;
                    }
                };
            }
        },
        ;

        abstract BiFunction<Object, Object, Object> cumulate(Object seed);
        private static boolean toBoolean(Object o) {
            if ( o == null) {
                return false;
            } else if (o instanceof Boolean) {
                return (Boolean) o;
            } else if (o instanceof String) {
                return Boolean.parseBoolean(o.toString());
            } else if (o instanceof Integer || o instanceof Long ) {
                return ((Number) o).intValue() != 0;
            } else if (o instanceof Number) {
                return ((Number) o).doubleValue() != 0;
            } else {
                return false;
            }
        }
        private static Long toLong(Object o) {
            if ( o == null) {
                return null;
            } else if (o instanceof Long) {
                return (Long) o;
            } else if (o instanceof Number ) {
                return ((Number) o).longValue();
            } else if (o instanceof Boolean) {
                return (long) (Boolean.TRUE.equals(o) ? 1 : 0);
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
            if ( o == null) {
                return null;
            } else if (o instanceof Double ) {
                return (Double) o;
            } else if (o instanceof Number ) {
                return ((Number) o).doubleValue();
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
        static BiFunction<Object, Object, Object> getCumulator(Object o) {
            if (o == null || o instanceof NullOrMissingValue) {
                return Cumulator.DROP.cumulate(o);
            } else if (o instanceof String) {
                return Cumulator.STRING.cumulate(o);
            } else if (o instanceof Character) {
                Character c = (Character) o;
                switch(c) {
                case '<':
                    return Cumulator.FIRST.cumulate(o);
                case '>':
                    return Cumulator.LAST.cumulate(o);
                case 'c':
                    return Cumulator.COUNT.cumulate(o);
                default:
                    return Cumulator.LIST.cumulate(o);
                }
            } else if (o instanceof Boolean && Boolean.TRUE.equals(o)) {
                return Cumulator.AND.cumulate(o);
            } else if (o instanceof Boolean && Boolean.TRUE.equals(! (Boolean) o)) {
                return Cumulator.OR.cumulate(o);
            } else if ((o instanceof Integer || o instanceof Long) && ((Number) o).longValue() == 0) {
                return Cumulator.ADD.cumulate(o);
            } else if ((o instanceof Integer || o instanceof Long) && ((Number) o).longValue() == 1) {
                return Cumulator.MULTIPLY.cumulate(o);
            } else if ((o instanceof Float || o instanceof Double) && ((Number) o).doubleValue() == 0) {
                return Cumulator.ADDFLOAT.cumulate(o);
            } else if ((o instanceof Float || o instanceof Double) && ((Number) o).longValue() == 1) {
                return Cumulator.MULTIPLYFLOAT.cumulate(o);
            } else if (o instanceof Collection || o.getClass().isArray()) {
                return Cumulator.LIST.cumulate(o);
            } else if (o instanceof Map ) {
                return Cumulator.MAP.cumulate(o);
            } else {
                return Cumulator.LIST.cumulate(o);
            }
        }
    }

    private static final Function<Event, Event> prepareEvent = i -> {
        i.forEach((key, value) -> {
            if (value instanceof StringBuilder)
                i.put(key, value.toString());
        });
        return i;
    };

    @Getter @Setter
    private Expression index;

    private Expression fire = null;

    private Object defaultSeedType = new Object[]{};

    private Map<String, Object> seeds = Collections.emptyMap();
    private Map<String, BiFunction<Object, Object, Object>> cumulators;
    private EventsRepository<Object> repository = null;
    private Processor expirationProcessor = new Identity();
    private Processor fireProcessor = new Identity();
    private int expiration = Integer.MAX_VALUE;
    private boolean forward = false;

    @Override
    public boolean configure(Properties properties) {
        if (index == null) {
            return false;
        }
        repository = new EventsRepository<>(properties);
        cumulators = new ConcurrentHashMap<>(seeds.size() + 1);
        // Default to timestamp is to keep the first
        cumulators.put("@timestamp", Cumulator.FIRST.cumulate(null));
        for (Entry<String, Object> i: seeds.entrySet()) {
            cumulators.put(i.getKey(), Cumulator.getCumulator(i.getValue()));
        }
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
        } catch (IllegalArgumentException | ProcessorException e) {
            // index key not found or expression failed, not to be merged
            return false;
        }
        // If the key is null, can't use the event
        if (eventKey == null) {
            return false;
        }
        logger.trace("key: {} for {}", eventKey, event);
        PausedEvent<Object> current = repository.getOrPause(eventKey, i -> getPausedEvent(event, i));
        // If we didn't get back the same event, we are actually merging the event.
        if (event != current.event) {
            synchronized (current) {
                logger.trace("merging {} in {}", event, current.event);
                for (Map.Entry<String, Object> i: event.entrySet()) {
                    String key = i.getKey();
                    Object last = current.event.get(key) instanceof NullOrMissingValue ? null : current.event.get(key);
                    Object next = i.getValue() instanceof NullOrMissingValue ? null : i.getValue();
                    BiFunction<Object, Object, Object> m =  cumulators.computeIfAbsent(key, j -> Cumulator.getCumulator(defaultSeedType));
                    Object newValue = m.apply(last, next);
                    if (newValue != null && ! (newValue instanceof NullOrMissingValue)) {
                        current.event.put(key, newValue);
                    }
                }
                // And don't forget the date, look for the @timestamp cumulator
                Date lastTimestamp = current.event.getTimestamp();
                Date nextTimestamp = event.getTimestamp();
                Object newTimestamp = cumulators.get(Event.TIMESTAMPKEY).apply(lastTimestamp, nextTimestamp);
                current.event.setTimestamp(newTimestamp);
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

    private PausedEvent<Object> getPausedEvent(Event event, Object key) {
        PausedEvent.Builder<Object> builder = PausedEvent.builder(event, key);
        PausedEvent<Object> pe = builder
                        .expiration(expiration, TimeUnit.SECONDS).onExpiration(expirationProcessor, prepareEvent)
                        .onSuccess(fireProcessor, prepareEvent)
                        .build()
                        ;
        // If the cumulators return a value, use it to initialize the new event time stamp
        // A null seed will keep it the new event timestamp all way long
        // '<' will keep the initial event timestamp
        // '>' will use the last event timestamp
        Object newTimestamp = cumulators.get(Event.TIMESTAMPKEY).apply(event.getTimestamp(), pe.event.getTimestamp());
        pe.event.setTimestamp(newTimestamp);
        return pe;
    }

    @Override
    public String getName() {
        return "Merge/" + index.getSource();
    }

    public Map<String, Object> getSeeds() {
        return seeds;
    }

    public void setSeeds(Map<String, Object> seeds) {
        this.seeds = seeds;
    }

    public Processor getOnExpire() {
        return expirationProcessor;
    }

    public void setOnExpiration(Processor expirationProcessor) {
        this.expirationProcessor = expirationProcessor;
    }

    public Processor getOnFire() {
        return fireProcessor;
    }

    public void setOnFire(Processor continueProcessor) {
        this.fireProcessor = continueProcessor;
    }

    public Expression getDoFire() {
        return fire;
    }

    public void setDoFire(Expression fire) {
        this.fire = fire;
    }

    /**
     * @return the expiration duration
     */
    public Integer getExpiration() {
        return expiration;
    }

    /**
     * @param expiration the expiration duration
     */
    public void setExpiration(Integer expiration) {
        this.expiration = expiration;
    }

    /**
     * @return the forward
     */
    public Boolean isForward() {
        return forward;
    }

    /**
     * @param forward the forward to set
     */
    public void setForward(Boolean forward) {
        this.forward = forward;
    }

    /**
     * @return the defaultSeed
     */
    public Object getDefault() {
        return defaultSeedType;
    }

    /**
     * @param defaultSeed the defaultSeed to set
     */
    public void setDefault(Object defaultSeed) {
        this.defaultSeedType = defaultSeed;
    }

    /**
     * @return the defaultSeed
     */
    public Object getDefaultMeta() {
        return defaultSeedType;
    }

}
