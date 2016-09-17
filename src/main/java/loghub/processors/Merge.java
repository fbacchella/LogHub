package loghub.processors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

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
                final StringBuilder builder = new StringBuilder();
                return (last, next) -> builder.length() == 0 ? builder.append(next) : builder.append(seed).append(next);
            }
        },
        LIST {
            @Override
            BiFunction<Object, Object, Object> cumulate(final Object seed) {
                final List<Object> list = new ArrayList<Object>();
                return (last, next) -> {if (next != null) list.add(next) ; return list;};
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
                return (last, next) -> next;
            }
        },
        FIRST {
            @Override
            BiFunction<Object, Object, Object> cumulate(Object seed) {
                return (last, next) -> last;
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
            if (o instanceof String) {
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
            } else if (o instanceof Collection || o instanceof Object[]) {
                return Cumulator.LIST.cumulate(o);
            } else if (o == null) {
                return Cumulator.DROP.cumulate(o);
            } else {
                return Cumulator.LIST.cumulate(o);
            }
        }
    }

    private String indexSource;
    private VarFormatter index;

    private String fireSource = null;
    private Expression fire = null;

    private Object defaultSeedName;
    private BiFunction<Object, Object, Object> defaultCumulator;

    private Map<String, Object> seeds = Collections.emptyMap();
    private Map<String, BiFunction<Object, Object, Object>> cumulators;
    private EventsRepository repository = null;
    private Processor timeoutProcessor = null;
    private Processor fireProcessor = null;
    private int timeout;
    private boolean forward;

    @Override
    public boolean configure(Properties properties) {
        if (indexSource == null) {
            return false;
        }
        repository = new EventsRepository(properties);
        cumulators = new ConcurrentHashMap<>(seeds.size());
        defaultCumulator = Cumulator.getCumulator(defaultSeedName);
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
        return super.configure(properties);
    }

    @Override
    public void process(Event event) throws ProcessorException {
        Object eventKey;
        try {
            eventKey = index.format(event);
        } catch (Exception e) {
            return;
        }
        // If the key is null, can't use the event
        if (eventKey == null) {
            return;
        }
        logger.debug("key: {} for {}", eventKey, event);
        PausedEvent current = repository.getOrPause(eventKey, () -> {
            return new PausedEvent(Event.emptyEvent())
                    .setTimeout(timeout, TimeUnit.SECONDS)
                    .onTimeout(timeoutProcessor, i -> {
                        i.setTimestamp(new Date());
                        i.entrySet().forEach(j -> {
                            if (j.getValue() instanceof StringBuilder)  i.put(j.getKey(), j.getValue().toString());
                        });
                        return i;
                    })
                    .onSuccess(fireProcessor, i -> {
                        i.setTimestamp(new Date());
                        i.entrySet().forEach(j -> {
                            if (j.getValue() instanceof StringBuilder)  i.put(j.getKey(), j.getValue().toString());
                        });
                        return i;
                    })
                    ;
        });
        synchronized (current) {
            for(Map.Entry<String, Object> i: event.entrySet()) {
                String key = i.getKey();
                Object last = current.event.get(key);
                Object next = event.get(key);
                BiFunction<Object, Object, Object> m =  cumulators.computeIfAbsent(key, j -> defaultCumulator);
                current.event.put(i.getKey(), m.apply(last, next));
            }
            if (fire != null) {
                Object dofire = fire.eval(current.event, Collections.emptyMap());
                if(dofire instanceof Boolean && ((Boolean) dofire)) {
                    repository.succed(eventKey);
                }
            }
        }
        if (! forward) {
            throw new ProcessorException.DroppedEventException(event);
        }
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
        return defaultSeedName;
    }

    /**
     * @param defaultSeed the defaultSeed to set
     */
    public void setDefault(Object defaultSeed) {
        this.defaultSeedName = defaultSeed;
    }

}
