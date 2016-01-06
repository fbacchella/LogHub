package loghub;

import java.util.Map;

public abstract class Decoder {

    public boolean configure(Map<String, Object> properties) {
        return true;
    }

    abstract public void decode(Event event, byte[] msg, int offset, int length);

    public void decode(Event event, byte[] msg) {
        decode(event, msg, 0, msg.length);
    }

}
