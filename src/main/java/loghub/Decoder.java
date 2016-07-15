package loghub;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.ReflectionUtil;

import loghub.configuration.Properties;

public abstract class Decoder {

    protected final Logger logger;
    
    protected Decoder() {
        logger = LogManager.getLogger(ReflectionUtil.getCallerClass(2));
    }

    public boolean configure(Properties properties) {
        return true;
    }

    abstract public Map<String, Object> decode(byte[] msg, int offset, int length);

    public Map<String, Object> decode(byte[] msg) {
        return decode(msg, 0, msg.length);
    }

}
