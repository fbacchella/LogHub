package loghub;

import java.util.Map;

public abstract class Decode {

    public void configure(Map<String, Object> properties) {

    }

    abstract public void decode(Event event, byte[] msg, int offset, int length);

    public void decode(Event event, byte[] msg) {
        decode(event, msg, 0, msg.length);
    }

}
