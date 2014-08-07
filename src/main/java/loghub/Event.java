package loghub;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Event implements Serializable {

    public Date timestamp;
    public String type;
    public Map<String, Object> properties = new HashMap<>();
    
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
        return super.toString();
    }
    
}
