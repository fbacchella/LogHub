package loghub;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import io.netty.buffer.ByteBuf;
import loghub.configuration.Properties;

public abstract class Decoder {

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

    protected Decoder() {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
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
