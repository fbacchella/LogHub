package loghub;

import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"decoder"})
public abstract class Receiver extends Thread implements Iterator<byte[]> {

    private static final Logger logger = LogManager.getLogger();

    private final NamedArrayBlockingQueue outQueue;
    protected Decoder decoder;

    public Receiver(NamedArrayBlockingQueue outQueue){
        setDaemon(true);
        setName("receiver-" + getReceiverName());
        this.outQueue = outQueue;
    }

    public boolean configure(Properties properties) {
        return true;
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
        while(! isInterrupted()) {
            Iterable<byte[]> stream = new Iterable<byte[]>() {
                @Override
                public Iterator<byte[]> iterator() {
                    Iterator<byte[]> i = Receiver.this.getIterator();
                    if(i == null) {
                        return new Iterator<byte[]>() {

                            @Override
                            public boolean hasNext() {
                                return false;
                            }

                            @Override
                            public byte[] next() {
                                return null;
                            }

                        };
                    } else {
                        return i;
                    }
                }
            };
            try {
                for(byte[] e: stream) {
                    logger.trace("new message received: {}", e);
                    if(e != null) {
                        eventseen++;
                        //Wrap, but not a problem, just count as 1
                        if(eventseen < 0) {
                            eventseen = 1;
                        }
                        Event event = new Event();
                        decode(event, e);
                        send(event);
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
                looptry++;
                logger.debug("event seen = 0, try = {}", looptry);
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
     * in charge of preparing the iterator.
     * @return an iterator or null in case of failure
     */
    protected Iterator<byte[]> getIterator() {
        try {
            startStream();
            return this;
        } catch (Exception e) {
            logger.error("unable to start receiver stream: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return null;
        }
    }

    protected void startStream() {
    };

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public byte[] next() {
        return null;
    }

    protected final void decode(Event event, byte[] msg) {
        decode(event, msg, 0, msg.length);
    }

    protected final void decode(Event event, byte[] msg, int offset, int size) {
        Map<String, Object> content = decoder.decode(msg, offset, size);
        if( content.containsKey(Event.TIMESTAMPKEY) && (event.get(Event.TIMESTAMPKEY) instanceof Date)) {
            event.timestamp = (Date) event.remove(Event.TIMESTAMPKEY);
        }
        if( content.containsKey(Event.TYPEKEY)) {
            event.type = event.remove(Event.TYPEKEY).toString();
        }
        content.entrySet().stream().forEach( i -> event.put(i.getKey(), i.getValue()));
    }

    /**
     * Send can be called directly
     * For listener that does asynchronous reception
     * @param event
     */
    protected final void send(Event event) {
        logger.debug("new event: {}", event);
        if(! outQueue.offer(event)) {
            logger.error("send failed for {}", event);
        } else {
            Stats.received.incrementAndGet();
        }
    }

    public Decoder getDecoder() {
        return decoder;
    }

    public void setDecoder(Decoder codec) {
        this.decoder = codec;
    }

    public abstract String getReceiverName();

}
