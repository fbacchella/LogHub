package loghub;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Event implements Serializable {
    
    private final static AtomicInteger KeyGenerator = new AtomicInteger(0);

    public Date timestamp;
    public String type;
    public Map<String, Object> properties = new HashMap<>();
    
    private final String key = Integer.toString(KeyGenerator.getAndIncrement());
    
    public Event() {
        timestamp = new Date();
    }

    public void put(String key, Object value) {
        properties.put(key, value);
    }

    @Override
    public String toString() {
        return type + "[" + timestamp + "]" + properties;
    }
    
    public String key() {
        return key;
    }
    
}
