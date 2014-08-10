package loghub;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Event implements Serializable {
    
    private final static AtomicLong KeyGenerator = new AtomicLong(0);

    public Date timestamp;
    public String type;
    public Map<String, Object> properties = new HashMap<>();
    private final byte[] key;
    
    public Event() {
        timestamp = new Date();
        long keyValue = KeyGenerator.getAndIncrement();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(keyValue);
        key = Arrays.copyOf(buffer.array(), 8);
    }

    public void put(String key, Object value) {
        properties.put(key, value);
    }

    @Override
    public String toString() {
        return type + "[" + timestamp + "]" + properties;
    }
    
    public byte[] key() {
        return key;
    }
    
}
