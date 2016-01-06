package loghub;

import loghub.configuration.Properties;

public abstract class Decoder {

    public boolean configure(Properties properties) {
        return true;
    }

    abstract public void decode(Event event, byte[] msg, int offset, int length);

    public void decode(Event event, byte[] msg) {
        decode(event, msg, 0, msg.length);
    }

}
