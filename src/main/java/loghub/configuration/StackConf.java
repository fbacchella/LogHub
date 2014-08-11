package loghub.configuration;

import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.events.Event;
import org.yaml.snakeyaml.events.Event.ID;
import org.yaml.snakeyaml.events.ScalarEvent;

public class StackConf  {
    
    public static class ParseException extends RuntimeException {
        public final Event event;
        public ParseException(String message, Event event, Throwable cause) {
            super(message, cause);
            this.event = event;
        }
        public ParseException(String message, Event event) {
            super(message);
            this.event = event;
        }

    }

    public class BeanInfo {
        public final String name;
        public final String value;
        BeanInfo(String name, String value) {
            this.name = name;
            this.value = value;
        }
        @Override
        public String toString() {
            return name + "=" + value;
        }
    }
    public enum State {
        EMPTY() {
            @Override
            public boolean isPop(ID id) {
                return false;
            }            
        },
        STREAM(Event.ID.StreamStart, Event.ID.StreamEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.StreamEnd;
            }                        
        },
        DOCUMENT(Event.ID.DocumentStart, Event.ID.DocumentEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.DocumentEnd;
            }
        },
        SLOTS(Event.ID.MappingStart, Event.ID.MappingEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.MappingEnd;
            }
        },
        SLOT_NAME(Event.ID.Scalar) {
            @Override
            public boolean isPop(ID id) {
                return false;
            }
        },
        SLOT_OBJECT_SEQUENCE(Event.ID.SequenceStart, Event.ID.SequenceEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.SequenceEnd;
            }
        },
        SLOT_OBJECT(Event.ID.MappingStart, Event.ID.Scalar, Event.ID.MappingEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.MappingEnd;
            }
        },
        CLASS_NAME(Event.ID.Scalar) {
            @Override
            public boolean isPop(ID id) {
                return false;
            }
        },
        CLASS_BEANS(Event.ID.MappingStart, Event.ID.MappingEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.MappingEnd;
            }
        },
        CLASS_BEANS_NAME(Event.ID.Scalar) {
            @Override
            public boolean isPop(ID id) {
                return false;
            }
        },
        CLASS_BEANS_VALUE(Event.ID.MappingStart, Event.ID.Scalar, Event.ID.MappingEnd) {
            @Override
            public boolean isPop(ID id) {
                return id == Event.ID.MappingEnd;
            }
        },
        ;
        public final Set<Event.ID> allowedTransitions;
        private State(Event.ID... allowedTransitions) {
            this.allowedTransitions = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(allowedTransitions)));
        }
        abstract public boolean isPop(Event.ID id);
    }

    private final Yaml yaml = new Yaml();

    private State state = State.EMPTY;
    private Event next = null;
    private final LinkedList<Event> eventStack = new LinkedList<>();
    private final LinkedList<State> stateStack = new LinkedList<>();
    private final Iterator<Event> cursor;

    public StackConf(Reader input) {
        //        for(Event n: yaml.parse(input)) {
        //           System.out.println(n); 
        //        }
        //System.out.println(yaml.load(input).toString());
        cursor = yaml.parse(input).iterator();
    }

    public State step(State... nextStates) throws ParseException {
        Event newEvent = cursor.next();
        if(state.isPop(getId(newEvent))) {
            state = stateStack.pop();
            next = eventStack.pop();
            return state;
        } else {
            for(State s: nextStates) {
                if(s.allowedTransitions.contains(getId(newEvent))) {
                    if(state != State.EMPTY && ! next.is(Event.ID.Scalar)) {
                        eventStack.push(next);
                        stateStack.push(state);                        
                    }
                    next = newEvent;
                    state = s;
                    return state;
                }
            } 
        }
        throw new ParseException("invalid state", next);
    }

    private Event.ID getId(Event e) throws ParseException {
        for(Event.ID i: Event.ID.values()) {
            if(e.is(i))
                return i;
        }
        throw new ParseException("unreachable code", next);
    }

    public  Event.ID getId() throws ParseException {
        return getId(next);
    }

    public String getValue() throws ParseException {
        if(next.is(ID.Scalar)) {
            return ((ScalarEvent)next).getValue();
        }
        throw new ParseException("not a scalar", next);
    }

    public <T> T doObject() throws ParseException {
        T t = null;
        switch(getId()) {
        case Scalar:
            t = newInstance(getValue());
            break;
        case MappingStart:
            step(State.CLASS_NAME);
            t = newInstance(getValue());
            step(State.CLASS_BEANS);
            break;
        default:
            throw new ParseException("Invalid object definition", next);
        }
        return t;
    }

    public Iterable<BeanInfo> getBeans() {
        return new Iterable<BeanInfo>(){
            @Override
            public Iterator<BeanInfo> iterator() {
                return new Iterator<BeanInfo>() {
                    // Set to null after iteration, so it can be called many time//
                    // because hasNext is not idempotent
                    // it was in stat CLASS_BEANS, it not, it will always be empty
                    Boolean notEmpty = StackConf.this.state == State.CLASS_BEANS;
                    Boolean hasNext = null;
                    @Override
                    public boolean hasNext() {
                        if(hasNext == null) {
                            hasNext = ( notEmpty && step(State.CLASS_BEANS_NAME) == State.CLASS_BEANS_NAME);
                        }
                        // Last iteration, one step if was a CLASS_BEANS
                        if(notEmpty && ! hasNext) {
                            step(State.SLOT_OBJECT);
                        }
                        return hasNext;
                    }

                    @Override
                    public BeanInfo next() {
                        //step(State.CLASS_BEANS_NAME);
                        String beanName = getValue();
                        step(State.CLASS_BEANS_VALUE);
                        String beanValue = null;
                        if(getId() == Event.ID.Scalar) {
                            beanValue = getValue();
                        }
                        hasNext = null;
                        return new BeanInfo(beanName, beanValue);
                    }

                    @Override
                    public void remove() {
                    }

                };
            };

        };
    }

    public <T> T doInstance() {
        stateStack.push(State.CLASS_NAME);
        state = State.CLASS_NAME;
        T t = newInstance(getValue());
        stateStack.pop();
        stateStack.pop();
        state = stateStack.getLast();
        return t;
    }

    @SuppressWarnings("unchecked")
    private <T> T newInstance(String className) {
        Class<T> objectClass;
        try {
            objectClass = (Class<T>) getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new ParseException("Unknown class", next);            
        }
        try {
            Constructor<T> objectConstructor = objectClass.getConstructor();
            return objectConstructor.newInstance();
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new ParseException("No valid constructor for " +  className, next, e);            
        }

    }

    public Event getEvent() {
        return next;
    }

    public String toString() {
        return eventStack.toString() + "/" + next + " -> " + stateStack.toString() + "/" + state;
    }

}
