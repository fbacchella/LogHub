package loghub.receivers;

import java.io.Closeable;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.DecodeException.RuntimeDecodeException;
import loghub.decoders.Decoder;
import loghub.metrics.Stats;
import loghub.security.AuthenticationHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

@Blocking(false)
public abstract class Receiver extends Thread implements Closeable {

    public abstract static class Builder<B extends Receiver> extends AbstractBuilder<B> {
        @Setter
        private Decoder decoder;
        @Setter
        private boolean withSSL = false;
        @Setter
        private ClientAuthentication SSLClientAuthentication = ClientAuthentication.NONE;
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
        @Setter
        private boolean blocking = true;
    }

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

    private PriorityBlockingQueue outQueue;
    private Pipeline pipeline;
    private final boolean blocking;
    protected final Decoder decoder;

    protected Receiver(Builder<? extends Receiver> builder){
        setDaemon(true);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        this.blocking = isBlocking() && builder.blocking;
        this.decoder = builder.decoder;
        this.withSSL = builder.withSSL;
        this.SSLClientAuthentication = builder.SSLClientAuthentication;
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

    @Override
    public void run() {
        AtomicLong eventseen = new AtomicLong();
        int looptry = 0;
        int wait = 100;
        while (! isInterrupted()) {
            getStream().forEach(e -> {
                try {
                    if (e != null) {
                        logger.trace("new message received: {}", e);
                        eventseen.incrementAndGet();
                        //Wrap, but not a problem, just count as 1
                        if (eventseen.get() < 0) {
                            eventseen.set(1);
                        }
                        send(e);
                    }
                } catch (Exception ex) {
                    logger.error("Failed received event: {}", Helpers.resolveThrowableException(ex));
                    logger.catching(Level.DEBUG, ex);
                }
            });
            // The previous loop didn't catch anything
            // So try some recovery
            if (eventseen.get() == 0) {
                looptry++;
                logger.debug("event seen = 0, try = {}", looptry);
                // A little magic, give the CPU to other threads
                Thread.yield();
                if (looptry > 3) {
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

    protected Stream<Event> getStream() {
        return Stream.empty();
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

    protected final Event mapToEvent(ConnectionContext<?> ctx, Map<String, Object> content) {
        if (content == null || content.isEmpty()) {
            manageDecodeException(new DecodeException("Received null or empty event"));
            Event.emptyEvent(ctx).end();
            return null;
        } else {
            try {
                Event newEvent;
                if (content instanceof Event) {
                    newEvent = (Event) content;
                } else if (content.size() == 1 && content.containsKey(Event.class.getCanonicalName())) {
                    // Special case, the message contain a loghub event, sent from another loghub
                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventContent = (Map<String, Object>) content.remove(Event.class.getCanonicalName());
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>) eventContent.remove("@fields");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
                    newEvent = Event.emptyEvent(ctx);
                    newEvent.putAll(fields);
                    Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
                            .filter(newEvent::setTimestamp)
                            .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
                    metas.forEach(newEvent::putMeta);
                } else {
                    newEvent = Event.emptyEvent(ctx);
                    Optional.ofNullable(content.get(timeStampField))
                            .filter(i -> i instanceof Date || i instanceof Instant || i instanceof Number)
                            .filter(newEvent::setTimestamp)
                            .ifPresent(ts -> content.remove(timeStampField));
                    content.entrySet().forEach(i -> newEvent.put(i.getKey(), i.getValue()));
                }
                if (newEvent.getConnectionContext() == null) {
                    Stats.newReceivedError(this, "Received an event without context");
                    newEvent.end();
                    return null;
                } else {
                    return newEvent;
                }
            } catch (RuntimeDecodeException ex) {
                Event.emptyEvent(ctx).end();
                manageDecodeException(ex.getDecodeException());
                return null;
            }
        }
    }

    protected final Stream<Event> decodeStream(ConnectionContext<?> ctx, byte[] msg, int offset, int size) {
        Stats.newReceivedMessage(this, size);
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
            return decoder.decode(ctx, buffer, bufferOffset, bufferSize).map(m -> mapToEvent(ctx, m)).filter(Objects::nonNull);
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
        Stats.newDecodError(this, Helpers.resolveThrowableException(ex));
        logger.debug("invalid message received: {}", ex.getMessage());
        logger.catching(Level.DEBUG, ex.getCause() != null ? ex.getCause() : ex);
    }

    /**
     * Send can be called directly
     * For listener that does asynchronous reception
     * @param event
     */
    protected final boolean send(Event event) {
        if (event == null) {
            manageDecodeException(new DecodeException("Received null event"));
            Event.emptyEvent(ConnectionContext.EMPTY).end();
            return false;
        } else if (event.getConnectionContext() == null) {
            Stats.newReceivedError(this, "Received an event without context");
            event.end();
            return false;
        } else {
            logger.trace("new event: {}", event);
            if(! event.inject(pipeline, outQueue, blocking)) {
                event.end();
                Stats.newBlockedError(this);
                logger.debug("Send failed from {}, pipeline destination {} blocked", this::getName, () -> pipeline.getName());
                return false;
            } else {
                Stats.newReceivedEvent(this);
                return true;
            }
        }
    }

    public abstract String getReceiverName();

    protected boolean withJaas() {
        return jaasName != null;
    }

    public void setOutQueue(PriorityBlockingQueue outQueue) {
        this.outQueue = outQueue;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

}
