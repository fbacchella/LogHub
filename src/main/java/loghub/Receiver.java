package loghub;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import loghub.configuration.Beans;
import zmq.ZMQHelper;

@Beans({"decoder"})
public abstract class Receiver extends Thread implements Iterator<Event> {

    private static final Logger logger = LogManager.getLogger();

    protected Socket pipe;
    private Map<byte[], Event> eventQueue;
    protected Decode decoder;
    private String endpoint;
    protected final SmartContext ctx;

    public Receiver(){
        setDaemon(true);
        setName("receiver-" + getReceiverName());
        ctx = SmartContext.getContext();
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }
    
    public void configure(Map<String, Object> properties) {
    }

    public void start(Map<byte[], Event> eventQueue) {
        this.eventQueue = eventQueue;
        pipe = ctx.newSocket(ZMQHelper.Method.CONNECT, ZMQHelper.Type.PUSH, endpoint);
        start();
    }

    /**
     * This method listen for new event and send it.
     * It can manage failure and retry. So it should be overridden with care.
     * @see java.lang.Thread#run()
     */
    public void run() {
        int eventseen = 0;
        int looptry = 0;
        int wait = 100;
        while(! isInterrupted() && ctx.isRunning()) {
            Iterable<Event> stream = new Iterable<Event>() {
                @Override
                public Iterator<Event> iterator() {
                    return Receiver.this.getIterator();
                }
            };
            try {
                for(Event e: stream) {
                    logger.trace("new message received: {}", e);
                    if(e != null) {
                        eventseen++;
                        //Wrap, but not a problem, just count as 1
                        if(eventseen < 0) {
                            eventseen = 1;
                        }
                        if( !ctx.isRunning()) {
                            break;
                        }
                        send(e);
                    }
                }
            } catch (Exception e) {
                eventseen = 0;
                logger.error(e.getMessage());
                logger.catching(e);
            }
            // The previous loop didn't catch anything
            // So try some recovery
            if(eventseen == 0) {
                // A little magic, give the CPU to other threads
                Thread.yield();
                if(looptry > 3) {
                    try {
                        Thread.sleep(wait);
                        wait = wait * 2;
                        looptry = 0;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } else {
                looptry = 0;
                wait = 0;
            }
        }
    }

    /**
     * This method call startStream and return this as an iterator. 
     * In this case, startStream will be called once and then hasNext and next will be used to iterate.
     * If overridden, startStream, hasNext and next methods will never be called, and the user is
     * in charge of preparing the iterator
     * @return
     */
    protected Iterator<Event> getIterator() {
        startStream();
        return this;
    }

    protected void startStream() {
    };

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Event next() {
        return null;
    }

    /**
     * Send can be called directly
     * For listener that does asynchronous reception
     * @param event
     */
    protected void send(Event event) {
        try {
            logger.debug("new event: {}", event);
            byte[] key = event.key();
            pipe.send(key);
            eventQueue.put(key, event);
        } catch (zmq.ZError.IOException | java.nio.channels.ClosedSelectorException | org.zeromq.ZMQException e ) {
            ctx.close(pipe);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public Decode getDecoder() {
        return decoder;
    }

    public void setDecoder(Decode codec) {
        this.decoder = codec;
    }

    public abstract String getReceiverName();

}
