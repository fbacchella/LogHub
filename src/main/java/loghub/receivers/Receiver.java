package loghub.receivers;

import java.io.Closeable;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Stats;
import loghub.Stats.PipelineStat;
import loghub.configuration.Properties;
import loghub.decoders.Decoder;
import loghub.decoders.Decoder.DecodeException;
import loghub.decoders.Decoder.RuntimeDecodeException;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;

@Blocking(false)
public abstract class Receiver extends Thread implements Iterator<Event>, Closeable {

    protected final Logger logger;

    private AuthenticationHandler authHandler = null;
    private boolean withSsl = false;
    private String sslclient = ClientAuthentication.NONE.name();
    private String sslKeyAlias = null;
    private String jaasName = null;
    private String user = null;
    private String password = null;
    private boolean useJwt = false;
    private String timeStampField = Event.TIMESTAMPKEY;

    private BlockingQueue<Event> outQueue;
    private Pipeline pipeline;
    private final boolean blocking;
    private Meter count;
    protected Decoder decoder = null;

    public Receiver(){
        setDaemon(true);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        blocking = isBlocking();
    }

    /**
     * <p>Check if the receiver should block or discard when destination is full.
     * Default is fault.</p>
     * 
     * <p>The base method check the value of the {@link Blocking annotation}
     * @return
     */
    protected boolean isBlocking() {
        return getClass().getAnnotation(Blocking.class).value();
    }

    public boolean configure(Properties properties) {
        setName("receiver." + getReceiverName());
        count = Properties.metrics.meter("receiver." + getReceiverName());
        if (decoder != null) {
            return decoder.configure(properties, this);
        } else if (getClass().getAnnotation(SelfDecoder.class) == null) {
            logger.error("Missing decoder");
            return false;
        } else {
            return true;
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
                logger.error("Failed received event: " + Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
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

    private Event decode(ConnectionContext<?> ctx, java.util.function.BooleanSupplier isValid, Supplier<Map<String, Object>> decoder) {
        if (! isValid.getAsBoolean()) {
            manageDecodeException(new DecodeException("received null or empty event"));
            Event.emptyEvent(ctx).end();
            return null;
        } else {
            try {
                Map<String, Object>  content = decoder.get();
                if (content == null) {
                    Event.emptyEvent(ctx).end();
                    manageDecodeException(new DecodeException("Received event with no usable body"));
                    return null;
                } else if (content instanceof Event) {
                    return (Event) content;
                } else {
                    Event event = Event.emptyEvent(ctx);
                    Optional.ofNullable(content.get(timeStampField))
                    .filter(i -> i instanceof Date || i instanceof Instant || i instanceof Number)
                    .ifPresent(ts -> {
                        if (event.setTimestamp(ts)) {
                            content.remove(timeStampField);
                        }
                    });
                    content.entrySet().stream().forEach( i -> event.put(i.getKey(), i.getValue()));
                    return event;
                }
            } catch (RuntimeDecodeException e) {
                Event.emptyEvent(ctx).end();
                manageDecodeException(e.getDecodeException());
                return null;
            }
        }
    }

    protected final Event decode(ConnectionContext<?> ctx, ByteBuf bbuf) {
        return decode(ctx, () -> bbuf != null && bbuf.isReadable(), () -> {
            try {
                return decoder.decode(ctx, bbuf);
            } catch (DecodeException e) {
                throw new RuntimeDecodeException(Helpers.resolveThrowableException(e), e);
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
                throw new RuntimeDecodeException(Helpers.resolveThrowableException(e), e);
            }
        });
    }

    protected void manageDecodeException(DecodeException ex) {
        Stats.newDecodError(ex);
        logger.debug("invalid message received: {}", ex.getMessage());
        logger.catching(Level.DEBUG, ex.getCause() != null ? ex.getCause() : ex);
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
            event.end();
            Stats.pipelineHanding(pipeline.getName(), PipelineStat.BLOCKIN);
            Stats.newBlockedError("Listener " + getName() + " sending to " + pipeline.getName());
            logger.debug("send failed from {}, pipeline destination {} blocked", () -> getName(), () -> pipeline.getName());
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

    public void setOutQueue(BlockingQueue<Event> outQueue) {
        this.outQueue = outQueue;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * @return the timeStampField
     */
    public String getTimeStampField() {
        return timeStampField;
    }

    /**
     * @param timeStampField the timeStampField to set
     */
    public void setTimeStampField(String timeStampField) {
        this.timeStampField = timeStampField;
    }

}
