package loghub;

import java.util.Map;

public abstract class Codec {

    abstract public void decode(Event event, byte[] msg);
    abstract public void configure(Map<String, Object> properties);

}
