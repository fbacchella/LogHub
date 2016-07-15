package loghub;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.ReflectionUtil;

import loghub.configuration.Properties;

public abstract class Decoder {

    public static class DecodeException extends Exception {
        public DecodeException(String message, Throwable cause) {
            super(message, cause);
        }
        DecodeException(String message) {
            super(message);
        }
    };

    protected final Logger logger;
    
    protected Decoder() {
        logger = LogManager.getLogger(ReflectionUtil.getCallerClass(2));
    }

    public boolean configure(Properties properties) {
        return true;
    }

    abstract public Map<String, Object> decode(byte[] msg, int offset, int length) throws DecodeException;

    public Map<String, Object> decode(byte[] msg) throws DecodeException{
        return decode(msg, 0, msg.length);
    }

}
