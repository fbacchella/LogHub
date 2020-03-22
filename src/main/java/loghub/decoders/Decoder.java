package loghub.decoders;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import io.netty.buffer.ByteBuf;
import loghub.AbstractBuilder;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.receivers.Receiver;
import lombok.Setter;

public abstract class Decoder {

    public abstract static class Builder<B extends Decoder> extends AbstractBuilder<B> {
        @Setter
        private String field = "message";
    };

    public static class DecodeException extends Exception {
        public DecodeException(String message, Throwable cause) {
            super(message, cause);
        }
        public DecodeException(String message) {
            super(message);
        }
    }

    public static class RuntimeDecodeException extends RuntimeException {
        public RuntimeDecodeException(DecodeException cause) {
            super(cause.getMessage(), cause);
        }
        public RuntimeDecodeException(String message, DecodeException cause) {
            super(message, cause);
        }
        public DecodeException getDecodeException() {
            return (DecodeException) getCause();
        }
    }

    private static final StackLocator stacklocator = StackLocator.getInstance();

    protected final Logger logger;

    protected final String field;
    protected Receiver receiver;

    protected Decoder(Builder<?  extends Decoder> builder) {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
        field = builder.field;
    }

    public boolean configure(Properties properties, Receiver receiver) {
        this.receiver = receiver;
        return true;
    }

    
    protected void manageDecodeException(ConnectionContext<?> connectionContext, DecodeException ex) {
        receiver.manageDecodeException(new DecodeException("received null or empty event"));
        Event.emptyEvent(connectionContext).end();
    }


    protected Object decodeObject(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        throw new UnsupportedOperationException();
    }

    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return decodeObject(ctx, msg, 0, msg.length);
    }

    protected Stream<Object> decodeStream(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return Stream.of(decodeObject(ctx, bbuf));
    }

    protected Stream<Object> decodeStream(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return Stream.of(decodeObject(ctx, msg, offset, length));
    }

    protected Stream<Object> decodeStream(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return Stream.of(decodeObject(ctx, msg, 0, msg.length));
    }

    private Stream<Map<String, Object>> resolve(ConnectionContext<?> ctx, Object o) throws DecodeException {
        if (o == null) {
            return Stream.empty();
        } else  if (o instanceof Collection){
            @SuppressWarnings("unchecked")
            Collection<Object> coll = (Collection<Object>) o;
            return coll.stream().map(getDecodeMap(ctx));
        } else if (o instanceof Iterable){
            @SuppressWarnings("unchecked")
            Iterable<Object> i = (Iterable<Object>) o;
            return StreamSupport.stream(i.spliterator(), false).map(getDecodeMap(ctx)).filter(m -> m != null);
        }  else {
            return Stream.of(decodeMap(ctx, o));
        }
    }

    private Function<Object, Map<String, Object>> getDecodeMap(ConnectionContext<?> ctx) {
        return  m -> {
            try {
                return decodeMap(ctx, m);
            } catch (DecodeException ex) {
                manageDecodeException(ctx, ex);
                return null;
            }
        };
    }

    private Map<String, Object> decodeMap(ConnectionContext<?> ctx, Object o) throws DecodeException {
        if (o instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) o;
            Map<String, Object> newMap = new HashMap<>(map.size());
            map.entrySet().stream().forEach(i -> newMap.put(i.getKey().toString(), i.getValue()));
            return newMap;
        } else {
            if (field != null) {
                return Collections.singletonMap(field, o);
            } else {
                throw new Decoder.DecodeException("Can't be mapped to event");
            }
        }
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return decodeStream(ctx, bbuf).flatMap(i -> {
            try {
                return resolve(ctx, i);
            } catch (DecodeException ex) {
                manageDecodeException(ctx, ex);
                return null;
            }
        });
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return decodeStream(ctx, msg, offset, length).flatMap(i -> {
            try {
                return resolve(ctx, i);
            } catch (DecodeException ex) {
                manageDecodeException(ctx, ex);
                return null;
            }
        });
    }

    public final Stream<Map<String, Object>> decode(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return decodeStream(ctx, msg).flatMap(i -> {
            try {
                return resolve(ctx, i);
            } catch (DecodeException ex) {
                manageDecodeException(ctx, ex);
                return null;
            }
        });
    }

}
