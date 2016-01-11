package loghub;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;
import loghub.processors.Drop;
import loghub.processors.Forker;

public class PipeStep extends Thread {

    public static final class EventWrapper extends Event {
        private final Event event;
        private String[] path;
        public EventWrapper(Event event) {
            this.event = event;
        }

        public void setProcessor(Processor processor) {
            String[] ppath = processor.getPathArray();
            path = Arrays.copyOf(ppath, ppath.length + 1);
        }

        @Override
        public Set<java.util.Map.Entry<String, Object>> entrySet() {
            return event.entrySet();
        }

        private Object action(BiFunction<String[], Object, Object> f, String key, final Object value) {
            final String[] lpath;
            if(key.startsWith(".")) {
                String[] tpath = key.substring(1).split(".");
                lpath = tpath.length == 0 ? new String[] {key.substring(1)} : tpath;
            } else {
                path[path.length - 1] = key;
                lpath = path;
            }
            return f.apply(lpath, value);
        }

        @Override
        public Object put(String key, Object value) {
            return action( ((i,j) -> event.put(i, j)), key, value);
        }

        @Override
        public void putAll(Map<? extends String, ? extends Object> m) {
            m.entrySet().stream().forEach( i-> { path[path.length - 1] = i.getKey(); event.put(path, i.getValue()); } );
        }

        @Override
        public Object get(Object key) {
            return action( ((i,j) -> event.get(i)), key.toString(), null);
        }

        @Override
        public Object remove(Object key) {
            return action( ((i,j) -> event.remove(i)), key.toString(), null);
        }

        @Override
        public boolean containsKey(Object key) {
            return (Boolean) action( ((i,j) -> event.containsKey(i)), key.toString(), null);
        }

        @Override
        public String toString() {
            return event.toString();
        }

    }

    private static final Logger logger = LogManager.getLogger();

    private NamedArrayBlockingQueue queueIn;
    private NamedArrayBlockingQueue queueOut;

    private final List<Processor> processors = new ArrayList<>();

    public PipeStep() {
        setDaemon(true);
    }

    public PipeStep(String name, int numStep, int width) {
        setDaemon(true);
        setName(name + "@" + numStep + "." + width);
    }

    public boolean configure(final Properties properties) {
        return processors.stream().allMatch(i -> i.configure(properties));
    }

    public void start(NamedArrayBlockingQueue endpointIn, NamedArrayBlockingQueue endpointOut) {
        logger.debug("starting {}", this);

        this.queueIn = endpointIn;
        this.queueOut = endpointOut;

        super.start();
    }

    public void addProcessor(Processor t) {
        processors.add(t);
    }

    public void run() {
        String threadName = getName();
        try {
            logger.debug("waiting on {}", queueIn.name);
            while(! isInterrupted()) {
                Event event = queueIn.take();
                logger.trace("{} received event {}", queueIn.name, event);
                try {
                    EventWrapper wevent = new EventWrapper(event);
                    for(Processor p: processors) {
                        setName(threadName + "-" + p.getName());
                        if(p instanceof Forker) {
                            ((Forker) p).fork(event);
                        } else if(p instanceof Drop) {
                            event.dropped = true;
                            continue;
                        } else {
                            wevent.setProcessor(p);
                            p.process(wevent);
                        }
                    }
                } catch (Exception e) {
                    logger.debug("failed to transform event {}", event);
                    logger.throwing(Level.DEBUG, e);
                    event.dropped = true;
                }
                setName(threadName);
                if( ! event.dropped) {
                    queueOut.put(event);
                    logger.trace("{} send event {}", () -> queueOut, () -> event);
                } else {
                    logger.error("{} dropped event {}", () -> queueOut, () -> event);
                }
            }
        } catch (InterruptedException e) {
            logger.debug("stop waiting on {}", queueIn.name);
        }
    }

    @Override
    public String toString() {
        return super.toString() + "." + processors.toString();
    }

}
