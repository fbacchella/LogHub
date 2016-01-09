package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

import loghub.configuration.Properties;
import loghub.processors.Forker;
import zmq.ZMQHelper;

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

    private Map<byte[], Event> eventQueue;
    private String endpointIn;
    private String endpointOut;

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

    public void start(Map<byte[], Event> eventQueue, String endpointIn, String endpointOut) {
        logger.debug("starting {}", this);

        this.eventQueue = eventQueue;
        this.endpointIn = endpointIn;
        this.endpointOut = endpointOut;

        super.start();
    }

    public void addProcessor(Processor t) {
        processors.add(t);
    }

    public void run() {
        SmartContext ctx = SmartContext.getContext();
        Socket in = ctx.newSocket(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PULL, endpointIn);
        Socket out = ctx.newSocket(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PUSH, endpointOut);
        String threadName = getName();
        boolean doclose = true;
        try {
            logger.debug("waiting on {}", () -> ctx.getURL(in));
            for(byte[] key: ctx.read(in)) {
                if( isInterrupted()) {
                    logger.debug("interrupted");
                    break;
                }
                Event event = eventQueue.remove(key);
                if(event == null) {
                    logger.warn("received a null event");
                    continue;
                }
                logger.trace("{} received event {}", () -> ctx.getURL(in), () -> event);
                try {
                    EventWrapper wevent = new EventWrapper(event);
                    for(Processor p: processors) {
                        if(p instanceof Forker) {
                            ((Forker) p).fork(event, eventQueue);
                            continue;
                        }
                        setName(threadName + "-" + p.getName());
                        wevent.setProcessor(p);
                        p.process(wevent);
                        if(event.dropped) {
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.debug("failed to transform event {}", event);
                    logger.throwing(Level.DEBUG, e);
                    event.dropped = true;
                }
                setName(threadName);
                if( ! event.dropped) {
                    eventQueue.put(key, event);
                    out.send(key);
                    logger.trace("{} send event {}", () -> ctx.getURL(out), () -> event);
                } else {
                    logger.trace("{} dropped event {}", () -> ctx.getURL(out), () -> event);                    
                }
            }
            logger.debug("stop waiting on {}", () -> ctx.getURL(in));
        } catch (zmq.ZError.CtxTerminatedException e ) {
            ZMQHelper.logZMQException(logger, "PipeStep", e);
            doclose = false;
        } catch (ZMQException | ZMQException.IOException | zmq.ZError.IOException e ) {
            ZMQHelper.logZMQException(logger, "PipeStep", e);
        } catch (IOException e) {
            logger.error("can't start reading: {}", e.getMessage());
            logger.throwing(Level.DEBUG, e);
        } finally {
            if(doclose) {
                ctx.close(out);
                ctx.close(in);
            }
        }
    }

    @Override
    public String toString() {
        return super.toString() + "." + processors.toString();
    }

}
