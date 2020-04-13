package loghub.receivers;

import java.io.Closeable;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Meter;

import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.Stats;
import loghub.Stats.PipelineStat;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.DecodeException.RuntimeDecodeException;
import loghub.decoders.Decoder;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

@Blocking(false)
public abstract class Receiver extends Thread implements Iterator<Event>, Closeable {

    @FunctionalInterface
    public static interface DecodeSupplier {
        Map<String, Object> get() throws DecodeException;
    }

    public abstract static class Builder<B extends Receiver> extends AbstractBuilder<B> {
        @Setter
        private Decoder decoder;
        @Setter
        private boolean withSSL = false;
        @Setter
        private String SSLClientAuthentication = ClientAuthentication.NONE.name();
        @Setter
        private String SSLKeyAlias;
        @Setter
        private String jaasName = null;
        @Setter
        private String user = null;
        @Setter
        private String password = null;
        @Setter
        private boolean useJwt = false;
        @Setter
        private String timeStampField = Event.TIMESTAMPKEY;
        @Setter
        private Filter filter;
    };

    protected final Logger logger;

    private AuthenticationHandler authHandler = null;
    @Getter
    private final boolean withSSL;
    @Getter
    private final ClientAuthentication SSLClientAuthentication;
    @Getter
    private final String jaasName;
    @Getter
    private final String user;
    @Getter
    private final String password;
    @Getter
    private final boolean useJwt;
    @Getter
    private final String timeStampField;
    @Getter
    private final String SSLKeyAlias;
    @Getter
    private final Filter filter;

    private BlockingQueue<Event> outQueue;
    private Pipeline pipeline;
    private final boolean blocking;
    private Meter count;
    protected final Decoder decoder;

    protected Receiver(Builder<?  extends Receiver> builder){
        setDaemon(true);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        blocking = isBlocking();
        this.decoder = builder.decoder;
        this.withSSL = builder.withSSL;
        this.SSLClientAuthentication = ClientAuthentication.valueOf(builder.SSLClientAuthentication.toUpperCase(Locale.ENGLISH));
        this.SSLKeyAlias = builder.SSLKeyAlias;
        this.jaasName = builder.jaasName;
        this.user = builder.user;
        this.password = builder.password;
        this.useJwt = builder.useJwt;
        this.timeStampField = builder.timeStampField;
        this.filter = builder.filter;
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
                            .setLogin(user).setPassword(password != null ? password.toCharArray() : null)
                            .setJaasName(jaasName).setJaasConfig(properties.jaasConfig)
                            .setJwtHandler(useJwt ? properties.jwtHandler : null).useJwt(useJwt)
                            .build();
        }
        return authHandler;
    }

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

    protected final Event mapToEvent(ConnectionContext<?> ctx, java.util.function.BooleanSupplier isValid, DecodeSupplier decoder) {
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
                } else if (content.size() == 1 && content.containsKey(Event.class.getCanonicalName())) {
                    // Special case, the message contain a loghub event, sent from another loghub
                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventContent = (Map<String, Object>) content.remove(Event.class.getCanonicalName());
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>)eventContent.remove("@fields");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
                    Event newEvent = Event.emptyEvent(ctx);
                    newEvent.putAll(fields);
                    Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
                    .filter(newEvent::setTimestamp)
                    .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
                    metas.forEach((i,j) -> newEvent.putMeta(i, j));
                    return newEvent;
                } else {
                    Event event = Event.emptyEvent(ctx);
                    Optional.ofNullable(content.get(timeStampField))
                    .filter(i -> i instanceof Date || i instanceof Instant || i instanceof Number)
                    .filter(event::setTimestamp)
                    .ifPresent(ts -> content.remove(timeStampField));
                    content.entrySet().stream().forEach( i -> event.put(i.getKey(), i.getValue()));
                    return event;
                }
            } catch (RuntimeDecodeException ex) {
                Event.emptyEvent(ctx).end();
                manageDecodeException(ex.getDecodeException());
                return null;
            } catch (DecodeException ex) {
                Event.emptyEvent(ctx).end();
                manageDecodeException(ex);
                return null;
            }
        }
    }

    protected final Stream<Event> decodeStream(ConnectionContext<?> ctx, byte[] msg, int offset, int size) {
        try {
            byte[] buffer;
            int bufferOffset;
            int bufferSize;
            if (filter != null) {
                buffer = filter.filter(msg, offset, size);
                bufferOffset = 0;
                bufferSize = msg.length;
            } else {
                buffer = msg;
                bufferOffset = offset;
                bufferSize = size;
            }
            return decoder.decode(ctx, msg, offset, size).map((m) -> mapToEvent(ctx, () -> buffer != null && bufferSize > 0 && bufferOffset < bufferSize, () -> m)).filter(Objects::nonNull);
        } catch (DecodeException ex) {
            manageDecodeException(ex);
            return Stream.of();
        } catch (FilterException e) {
            manageDecodeException(new DecodeException(e));
            return Stream.of();
        }
    }

    protected final Stream<Event> decodeStream(ConnectionContext<?> ctx, byte[] msg) {
        return decodeStream(ctx, msg, 0, msg.length);
    }

    public void manageDecodeException(DecodeException ex) {
        Stats.newDecodError(Helpers.resolveThrowableException(ex));
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

    public abstract String getReceiverName();

    protected boolean withJaas() {
        return jaasName != null;
    }

    public void setOutQueue(BlockingQueue<Event> outQueue) {
        this.outQueue = outQueue;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

}
