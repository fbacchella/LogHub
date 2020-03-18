package loghub.decoders;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import io.netty.buffer.ByteBuf;
import loghub.AbstractBuilder;
import loghub.ConnectionContext;
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

    protected Decoder(Builder<?  extends Decoder> builder) {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
        field = builder.field;
    }

    public boolean configure(Properties properties, Receiver receiver) {
        return true;
    }

    protected Map<String, Object> decode(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException {
        return Collections.emptyMap();
    }

    protected Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return Collections.emptyMap();
    }

    protected Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return decode(ctx, msg, 0, msg.length);
    }

    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return Stream.of(decode(ctx, bbuf));
    }

    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return Stream.of(decode(ctx, msg, offset, length));
    }

    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return Stream.of(decode(ctx, msg, 0, msg.length));
    }

}
