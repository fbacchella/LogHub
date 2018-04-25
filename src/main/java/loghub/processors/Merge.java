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

import loghub.ConnectionContext;
import loghub.Event;
import loghub.EventsRepository;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.PausedEvent;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.configuration.Properties;

public class Merge extends Processor {

    private enum Cumulator {
        STRING {
            @Override
            BiFunction<Object, Object, Object> cumulate(final Object seed) {
                final String stringSeed = seed == null ? "" : seed.toString();
                return (last, next) -> last == null ? new StringBuilder().append(next).toString() : new StringBuilder(last.toString()).append(stringSeed).append(next).toString();
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
                    newList = new ArrayList<>();
                    Object[] seedArray = (Object[]) source;
                    newList.addAll(Arrays.asList(seedArray));
                } else {
                    newList = new ArrayList<>();
                    newList.add(source);
                }
                return newList;
            }
            @Override
            BiFunction<Object, Object, Object> cumulate(final Object seed) {
                final List<Object> listSeed = object2list(seed);
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
                    return new HashMap<Object, Object>();
                } else if (source instanceof Map) {
                    return (Map<Object, Object>) source;
                } else {
                    // Can't fill a map with a single value, a key is needed
                    return new HashMap<Object, Object>();
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
                        return new StringBuilder(last.toString()).append(next.toString());
                    } else if (last instanceof StringBuilder) {
                        return ((StringBuilder)last).append(next.toString());
                    } else if (last instanceof Number && next instanceof Number) {
                        return ((Number)last).longValue() + ((Number)next).longValue();
                    } else {
                        return next;
                    }
                };
            }
        },
        ;

        abstract BiFunction<Object, Object, Object> cumulate(final Object seed);
        private static boolean toBoolean(Object o) {
            if ( o == null) {
                return false;
            } else if (o instanceof Boolean) {
                return (Boolean) o;
            } else if (o instanceof String) {
                return Boolean.valueOf(o.toString());
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
                return (long) ((Boolean) o ? 1 : 0);
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
                return (double) ((Boolean) o ? 1.0 : 0.0);
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
            if (o == null) {
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
            } else if (o instanceof Boolean && (Boolean) o) {
                return Cumulator.AND.cumulate(o);
            } else if (o instanceof Boolean && ! (Boolean) o) {
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
        i.entrySet().forEach(j -> {
            if (j.getValue() instanceof StringBuilder) i.put(j.getKey(), j.getValue().toString());
        });
        return i;
    };

    private String indexSource;
    private VarFormatter index;

    private String fireSource = null;
    private Expression fire = null;

    private Object defaultSeedType = new Object[]{};

    private Map<String, Object> seeds = Collections.emptyMap();
    private Map<String, BiFunction<Object, Object, Object>> cumulators;
    private EventsRepository<String> repository = null;
    private Processor timeoutProcessor = new Identity();
    private Processor fireProcessor = new Identity();
    private int timeout = Integer.MAX_VALUE;
    private boolean forward = false;
    private String nextPipeline;

    @Override
    public boolean configure(Properties properties) {
        if (indexSource == null) {
            return false;
        }
        repository = new EventsRepository<String>(properties);
        cumulators = new ConcurrentHashMap<>(seeds.size() + 1);
        // Default to timestamp is to keep the first
        cumulators.put("@timestamp", Cumulator.FIRST.cumulate(null));
        for (Entry<String, Object> i: seeds.entrySet()) {
            cumulators.put(i.getKey(), Cumulator.getCumulator(i.getValue()));
        }
        index = new VarFormatter(indexSource);
        // Prepare fire only if test and processor given for that
        if (fireSource != null && fireProcessor != null) {
            try {
                fire = new Expression(fireSource, properties.groovyClassLoader, properties.formatters);
            } catch (ExpressionException e) {
                Expression.logError(e, fireSource, logger);
                return false;
            } 
            if (! fireProcessor.configure(properties)) {
                return false;
            }
        }
        if (timeoutProcessor != null && ! timeoutProcessor.configure(properties)) {
            return false;
        }
        if (nextPipeline == null) {
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        String eventKey;
        try {
            eventKey = index.format(event);
        } catch (IllegalArgumentException e) {
            // index key not found, not to be merged
            return false;
        }
        // If the key is null, can't use the event
        if (eventKey == null) {
            return false;
        }
        logger.debug("key: {} for {}", eventKey, event);
        PausedEvent<String> current = repository.getOrPause(eventKey, () -> {
            PausedEvent<String> pe = new PausedEvent<String>(event.isTest() ? Event.emptyTestEvent(ConnectionContext.EMPTY) : Event.emptyEvent(ConnectionContext.EMPTY), eventKey)
                    .setTimeout(timeout, TimeUnit.SECONDS).onTimeout(timeoutProcessor, prepareEvent)
                    .onSuccess(fireProcessor, prepareEvent)
                    .setPipeline(nextPipeline)
                    ;
            // If the cumulators return a value, use it to initialize the new event time stamp
            // A null seed will keep it the new event timestamp all way long
            // '<' will keep the initial event timestamp
            // '>' will use the last event timestamp
            Object newTimestamp = cumulators.get("@timestamp").apply(event.getTimestamp(), pe.event.getTimestamp());
            if (newTimestamp instanceof Date) {
                pe.event.setTimestamp((Date)newTimestamp);
            }
            return pe;
        });
        synchronized (current) {
            logger.trace("merging {} in {}", event, current.event);
            for(Map.Entry<String, Object> i: event.entrySet()) {
                String key = i.getKey();
                Object last = current.event.get(key);
                Object next = i.getValue();
                BiFunction<Object, Object, Object> m =  cumulators.computeIfAbsent(key, j -> Cumulator.getCumulator(defaultSeedType));
                Object newValue = m.apply(last, next);
                if (newValue != null) {
                    current.event.put(key, newValue);
                }
            }
            // And don't forget the date, look for the @timestamp cumulator
            Date lastTimestamp = current.event.getTimestamp();
            Date nextTimestamp = event.getTimestamp();
            Object newTimestamp = cumulators.get("@timestamp").apply(lastTimestamp, nextTimestamp);
            if (newTimestamp instanceof Date) {
                current.event.setTimestamp((Date)newTimestamp);
            }

            if (fire != null) {
                Object dofire = fire.eval(current.event);
                if(dofire instanceof Boolean && ((Boolean) dofire)) {
                    repository.succed(eventKey);
                }
            }
        }
        if (! forward) {
            throw new ProcessorException.DroppedEventException(event);
        }
        return true;
    }

    @Override
    public String getName() {
        return "Merge/" + indexSource;
    }

    public Map<String, Object> getSeeds() {
        return seeds;
    }

    public void setSeeds(Map<String, Object> seeds) {
        this.seeds = seeds;
    }

    public String getIndex() {
        return indexSource;
    }

    public void setIndex(String index) {
        this.indexSource = index;
    }

    public Processor getOnTimeout() {
        return timeoutProcessor;
    }

    public void setOnTimeout(Processor timeoutProcessor) {
        this.timeoutProcessor = timeoutProcessor;
    }

    public Processor getOnFire() {
        return fireProcessor;
    }

    public void setOnFire(Processor continueProcessor) {
        this.fireProcessor = continueProcessor;
    }

    public String getDoFire() {
        return fireSource;
    }

    public void setDoFire(String fireSource) {
        this.fireSource = fireSource;
    }

    /**
     * @return the timeout
     */
    public Integer getTimeout() {
        return timeout;
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
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
     * @return the pipeline that will process the event
     */
    public String getInPipeline() {
        return nextPipeline;
    }

    /**
     * @param nextPipeline the pipeline that will process the event
     */
    public void setInPipeline(String nextPipeline) {
        this.nextPipeline = nextPipeline;
    }

}
