package loghub;

import java.util.Map;

public abstract class Decode {

    public void configure(Map<String, Object> properties) {
        
    }

    abstract public void decode(Event event, byte[] msg);

}
