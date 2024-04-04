package loghub.receivers;

import java.io.Closeable;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.security.auth.login.Configuration;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Pipeline;
import loghub.PriorityBlockingQueue;
import loghub.configuration.Properties;
import loghub.decoders.DecodeException;
import loghub.decoders.DecodeException.RuntimeDecodeException;
import loghub.decoders.Decoder;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;
import loghub.security.AuthenticationHandler;
import loghub.security.JWTHandler;
import loghub.security.ssl.ClientAuthentication;
import lombok.Getter;
import lombok.Setter;

@Blocking(false)
public abstract class Receiver<R extends Receiver<R, B>, B extends Receiver.Builder<R, B>> extends Thread implements Closeable {

    public abstract static class Builder<R extends Receiver<R, B>, B extends Builder<R, B>> extends AbstractBuilder<R> {
        @Setter
        protected Decoder decoder;
        @Setter
        protected boolean withSSL = false;
        @Setter
        protected ClientAuthentication SSLClientAuthentication = ClientAuthentication.NONE;
        @Setter
        protected String SSLKeyAlias;
        @Setter
        protected SSLContext sslContext;
        @Setter
        protected SSLParameters sslParams;
        @Setter
        protected String jaasName = null;
        @Setter
        protected String user = null;
        @Setter
        protected String password = null;
        @Setter
        protected boolean useJwt = false;
        @Setter
        protected JWTHandler jwtHandler;
        @Setter
        protected Configuration jaasConfig;
        @Setter
        protected String timeStampField = Event.TIMESTAMPKEY;
        @Setter
        protected Filter filter;
        @Setter
        protected boolean blocking = true;
    }

    protected final Logger logger;

    @Getter
    private final boolean withSSL;
    @Getter
    private final SSLContext sslContext;
    @Getter
    private final SSLParameters sslParams;
    @Getter
    private final String SSLKeyAlias;
    @Getter
    private final ClientAuthentication SSLClientAuthentication;
    @Getter
    private final String timeStampField;
    @Getter
    private final Filter filter;
    @Getter
    private final AuthenticationHandler authenticationHandler;

    private PriorityBlockingQueue outQueue;
    private Pipeline pipeline;
    private final boolean blocking;
    protected final Decoder decoder;
    @Getter
    private EventsFactory eventsFactory;

    protected Receiver(B builder){
        setDaemon(true);
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        this.blocking = isBlocking() && builder.blocking;
        if (getClass().getAnnotation(SelfDecoder.class) != null) {
            if (builder.decoder != null) {
                throw new IllegalArgumentException("Decoder " + builder.decoder.getClass().getName().toString() + " will be ignored, this receiver handle decoding");
            }
            this.decoder = null;
        } else {
            this.decoder = builder.decoder;
        }
        this.withSSL = builder.withSSL;
        this.sslContext = builder.sslContext;
        this.sslParams = builder.sslParams;
        this.SSLClientAuthentication = builder.SSLClientAuthentication;
        this.SSLKeyAlias = builder.SSLKeyAlias;
        this.timeStampField = builder.timeStampField;
        this.filter = builder.filter;
        this.authenticationHandler = buildAuthenticationHandler(builder);
    }

    /**
     * <p>Check if the receiver should block or discard when destination is full.
     * Default is fault.</p>
     * 
     * <p>The base method check the value of the {@link Blocking annotation}
     * @return true if the receiver block.
     */
    protected boolean isBlocking() {
        return getClass().getAnnotation(Blocking.class).value();
    }

    public boolean configure(Properties properties) {
        eventsFactory = properties.eventsFactory;
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

    protected AuthenticationHandler buildAuthenticationHandler(B builder) {
        if ((builder.user != null && builder.password != null) || (builder.jaasName != null && builder.jaasConfig != null) || builder.useJwt) {
            return AuthenticationHandler
                           .getBuilder()
                           .setLogin(builder.user).setPassword(builder.password != null ? builder.password.toCharArray() : null)
                           .setJaasName(builder.jaasName).setJaasConfig(builder.jaasConfig)
                           .setJwtHandler(builder.useJwt ? builder.jwtHandler : null).useJwt(builder.useJwt)
                           .build();

        } else {
            return null;
        }
     }

    @Override
    public void run() {
        AtomicBoolean eventSeen = new AtomicBoolean();
        while (! isInterrupted()) {
            eventSeen.set(false);
            getStream().forEach(e -> {
                try {
                    if (e != null) {
                        logger.trace("new message received: {}", e);
                        eventSeen.set(true);
                        send(e);
                    }
                } catch (Exception ex) {
                    logger.error("Failed received event: {}", Helpers.resolveThrowableException(ex));
                    logger.catching(Level.DEBUG, ex);
                }
            });
            // Avoid a wild loop for a sleeping receiver, a little sleep
            if (eventSeen.get()) {
                try {
                    Thread.sleep(100);
                 } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
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
            EventsFactory.deadEvent(ctx);
            return null;
        } else {
            try {
                Event newEvent;
                if (content instanceof Event) {
                    newEvent = (Event) content;
                } else if (content.size() == 1 && content.containsKey(Event.EVENT_ENTRY)) {
                    // Special case, the message contain a loghub event, sent from another loghub
                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventContent = (Map<String, Object>) content.remove(Event.EVENT_ENTRY);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>) eventContent.remove("@fields");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
                    newEvent = eventsFactory.newEvent(ctx);
                    newEvent.putAll(fields);
                    Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
                            .filter(newEvent::setTimestamp)
                            .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
                    metas.forEach(newEvent::putMeta);
                } else {
                    newEvent = eventsFactory.newEvent(ctx);
                    Optional.ofNullable(content.get(timeStampField))
                            .filter(i -> i instanceof Date || i instanceof Instant || i instanceof Number)
                            .filter(newEvent::setTimestamp)
                            .ifPresent(ts -> content.remove(timeStampField));
                    newEvent.putAll(content);
                }
                if (newEvent.getConnectionContext() == null) {
                    Stats.newReceivedError(this, "Received an event without context");
                    newEvent.end();
                    return null;
                } else {
                    return newEvent;
                }
            } catch (RuntimeDecodeException ex) {
                EventsFactory.deadEvent(ctx);
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
            EventsFactory.deadEvent(ConnectionContext.EMPTY);
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

    public void setOutQueue(PriorityBlockingQueue outQueue) {
        this.outQueue = outQueue;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

}
