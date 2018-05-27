package loghub;

import java.io.Closeable;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;

import io.netty.buffer.ByteBuf;
import loghub.Decoder.DecodeException;
import loghub.Decoder.RuntimeDecodeException;
import loghub.configuration.Properties;
import loghub.receivers.Blocking;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;

public abstract class Receiver extends Thread implements Iterator<Event>, Closeable {

    /**
     * Any receiver that does it's own decoding should set the decoder to this value during configuration
     */
    protected static final Decoder NULLDECODER = new Decoder() {
        @Override
        public Map<String, Object> decode(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException {
            return null;
        }
        @Override
        public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
            return null;
        }

    };

    protected final Logger logger;

    private AuthenticationHandler authHandler = null;
    private boolean withSsl = false;
    private String sslclient = ClientAuthentication.NONE.name();
    private String sslKeyAlias = null;
    private String jaasName = null;
    private String user = null;
    private String password = null;
    private boolean useJwt = false;

    private final BlockingQueue<Event> outQueue;
    private final Pipeline pipeline;
    private final boolean blocking;
    private Meter count;
    protected Decoder decoder = null;

    public Receiver(BlockingQueue<Event> outQueue, Pipeline pipeline){
        setDaemon(true);
        this.outQueue = outQueue;
        this.pipeline = pipeline;
        logger = LogManager.getLogger(Helpers.getFistInitClass());
        blocking = getClass().getAnnotation(Blocking.class) != null;
    }

    public boolean configure(Properties properties) {
        setName("receiver-" + getReceiverName());
        count = Properties.metrics.meter("receiver." + getReceiverName());
        if (decoder != null) {
            return decoder.configure(properties, this);
        } else {
            logger.error("Missing decoder");
            return false;
        }
    }

    protected AuthenticationHandler getAuthHandler(Properties properties) {
        if (authHandler == null) {
            authHandler = AuthenticationHandler.getBuilder()
                            .setSslClientAuthentication(sslclient).useSsl(withSsl)
                            .setLogin(user).setPassword(password != null ? password.toCharArray() : null)
                            .setJaasName(jaasName).setJaasConfig(properties.jaasConfig)
                            .setJwtHandler(useJwt ? properties.jwtHandler : null).useJwt(useJwt)
                            .build();
        }
        return authHandler;
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
        while (! isInterrupted()) {
            Iterable<Event> stream = new Iterable<Event>() {
                @Override
                public Iterator<Event> iterator() {
                    Iterator<Event> i = Receiver.this.getIterator();
                    if (i == null) {
                        return Helpers.getEmptyIterator();
                    } else {
                        return i;
                    }
                }
            };
            try {
                for (Event e: stream) {
                    if (e != null) {
                        logger.trace("new message received: {}", e);
                        eventseen++;
                        //Wrap, but not a problem, just count as 1
                        if(eventseen < 0) {
                            eventseen = 1;
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
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            } else {
                looptry = 0;
                wait = 0;
            }
        }
        close();
    }

    /**
     * This empty method is called when processing is stopped, it should be
     * overridden for clean up and called by the receiving thread. It usually called
     * as the last instruction of the {@link java.lang.Thread#run()} method
     */
    public void close() {

    }

    /**
     * This method is used when an external thread wants a receiver to stop.
     */
    public void stopReceiving() {
        interrupt();
    }

    /**
     * This method call startStream and return this as an iterator. 
     * In this case, startStream will be called once and then hasNext and next will be used to iterate.
     * If overridden, startStream, hasNext and next methods will never be called, and the user is
     * in charge of preparing the iterator.
     * @return an iterator or null in case of failure
     */
    protected Iterator<Event> getIterator() {
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
    public Event next() {
        throw new NoSuchElementException();
    }

    private Event decode(ConnectionContext<?> ctx,java.util.function.BooleanSupplier isValid, Supplier<Map<String, Object>> decoder) {
        EventInstance event = new EventInstance(ctx);
        if ( ! isValid.getAsBoolean()) {
            logger.info("received null or empty event");
            event.end();
            return null;
        } else {
            try {
                Map<String, Object> content = decoder.get();
                if (content.containsKey(Event.TIMESTAMPKEY) && (event.get(Event.TIMESTAMPKEY) instanceof Date)) {
                    event.setTimestamp((Date) event.remove(Event.TIMESTAMPKEY));
                }
                content.entrySet().stream().forEach( i -> event.put(i.getKey(), i.getValue()));
            } catch (RuntimeDecodeException e) {
                manageDecodeException((DecodeException)e.getCause());
                event.end();
                return null;
            }
        }
        return event;
    }

    protected final Event decode(ConnectionContext<?> ctx, ByteBuf bbuf) {
        return decode(ctx, () -> bbuf != null && bbuf.isReadable(), () -> {
            try {
                return decoder.decode(ctx, bbuf);
            } catch (DecodeException e) {
                throw new RuntimeDecodeException(e.getMessage(), e.getCause());
            }
        });
    }

    protected final Event decode(ConnectionContext<?> ctx, byte[] msg) {
        return decode(ctx, msg, 0, msg != null ? msg.length : 0);
    }

    protected final Event decode(ConnectionContext<?> ctx, byte[] msg, int offset, int size) {
        return decode(ctx, () -> msg != null && size > 0 && offset < size, () -> {
            try {
                return decoder.decode(ctx, msg, offset, size);
            } catch (DecodeException e) {
                throw new RuntimeDecodeException(e.getMessage(), e.getCause());
            }
        });
    }

    protected final Event emptyEvent(ConnectionContext<?> ctx) {
        return new EventInstance(ctx);
    }

    protected void manageDecodeException(DecodeException ex) {
        Stats.newDecodError(ex);
        logger.error("invalid message received: {}", ex.getMessage());
        logger.throwing(Level.DEBUG, ex.getCause() != null ? ex.getCause() : ex);
    }

    /**
     * Send can be called directly
     * For listener that does asynchronous reception
     * @param event
     */
    protected final boolean send(Event event) {
        count.mark();
        logger.debug("new event: {}", event);
        Stats.received.incrementAndGet();
        if(! event.inject(pipeline, outQueue, blocking)) {
            Stats.dropped.incrementAndGet();
            Properties.metrics.meter("Pipeline." + pipeline.getName() + ".blocked.in").mark();
            event.end();
            logger.error("send failed for {}, destination blocked", event);
            return false;
        } else {
            return true;
        }
    }

    public Decoder getDecoder() {
        return decoder;
    }

    public void setDecoder(Decoder codec) {
        this.decoder = codec;
    }

    public abstract String getReceiverName();

    /**
     * @return the withSsl
     */
    public boolean isWithSSL() {
        return withSsl;
    }

    /**
     * @param withSsl the withSsl to set
     */
    public void setWithSSL(boolean withSsl) {
        this.withSsl = withSsl;
    }

    /**
     * @return the sslclient
     */
    public String getSSLClientAuthentication() {
        return sslclient;
    }

    /**
     * @param sslclient the sslclient to set
     */
    public void setSSLClientAuthentication(String sslclient) {
        this.sslclient = sslclient;
    }

    protected boolean withJaas() {
        return jaasName != null;
    }

    public String getJaasName() {
        return jaasName;
    }

    public void setJaasName(String jaasName) {
        this.jaasName = jaasName;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the useJwt
     */
    public boolean isUseJwt() {
        return useJwt;
    }

    /**
     * @param useJwt the useJwt to set
     */
    public void setUseJwt(boolean useJwt) {
        this.useJwt = useJwt;
    }

    public String getSSLKeyAlias() {
        return sslKeyAlias;
    }

    public void setSSLKeyAlias(String sslKeyAlias) {
        this.sslKeyAlias = sslKeyAlias;
    }

}
