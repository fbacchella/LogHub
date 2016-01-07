package loghub;

import java.util.Map;

import loghub.configuration.Properties;

public abstract class Decoder {

    public boolean configure(Properties properties) {
        return true;
    }

    abstract public Map<String, Object> decode(byte[] msg, int offset, int length);

    public Map<String, Object> decode(byte[] msg) {
        return decode(msg, 0, msg.length);
    }

}
