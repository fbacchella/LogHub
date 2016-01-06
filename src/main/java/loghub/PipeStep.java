package loghub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import zmq.ZMQHelper;

import org.zeromq.ZMQException;

public class PipeStep extends Thread {

    private static final Logger logger = LogManager.getLogger();

    private Map<byte[], Event> eventQueue;
    private String endpointIn;
    private String endpointOut;

    private final List<Transformer> transformers = new ArrayList<>();

    public PipeStep() {
        setDaemon(true);
    }

    public PipeStep(String name, int numStep, int width) {
        setDaemon(true);
        setName(name + "@" + numStep + "." + width);
    }

    public boolean configure(final Map<String, Object> properties) {
        return transformers.stream().allMatch(i -> i.configure(properties));
    }

    public void start(Map<byte[], Event> eventQueue, String endpointIn, String endpointOut) {
        logger.debug("starting {}", this);

        this.eventQueue = eventQueue;
        this.endpointIn = endpointIn;
        this.endpointOut = endpointOut;

        super.start();
    }

    public void addTransformer(Transformer t) {
        transformers.add(t);
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
                    for(Transformer t: transformers) {
                        setName(threadName + "-" + t.getName());
                        t.transform(event);
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
        return super.toString() + "." + transformers.toString();
    }

}
