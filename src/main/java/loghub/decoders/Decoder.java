package loghub.decoders;

import java.util.Map;

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
        public RuntimeDecodeException(String message, Throwable cause) {
            super(message, cause);
        }
        public RuntimeDecodeException(String message) {
            super(message);
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

    public abstract Map<String, Object> decode(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length) throws DecodeException;

    public abstract Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException;

    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg) throws DecodeException{
        return decode(ctx, msg, 0, msg.length);
    }

}
