package loghub;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class Event extends HashMap<String, Object> implements Serializable {

    private final static AtomicLong KeyGenerator = new AtomicLong(0);

    public static final String TIMESTAMPKEY = "__timestamp";
    public static final String TYPEKEY = "__type";

    public Date timestamp;
    public String type;
    private final byte[] key;
    public boolean dropped = false;

    public Event() {
        super();
        timestamp = new Date();
        long keyValue = KeyGenerator.getAndIncrement();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(keyValue);
        key = Arrays.copyOf(buffer.array(), 8);
    }

    @Override
    public String toString() {
        return type + "[" + timestamp + "]" + super.toString();
    }

    public byte[] key() {
        return key;
    }

    @Override
    @Deprecated
    public Object put(String key, Object value) {
        throw new UnsupportedOperationException("don't call put from a processor");
    }

    @SuppressWarnings("unchecked")
    public Object put(String prefix, String key, Object value) {
        if(prefix == null || prefix.isEmpty()) {
            super.put(key, value);
        }
        key = prefix + "." + key;
        Map<String, Object> current = this;
        String[] path = key.split("\\.");
        // First iteration, needs to call super.put()
        if(! current.containsKey(path[0]) || ! (current.get(path[0]) instanceof Map) ) {
            super.put(path[0], new HashMap<String, Object>());
        }
        current = (Map<String, Object>) current.get(path[0]);
        for(int i = 1; i < path.length - 1 ; i++) {
            System.out.println(path[i] + " " + current.get(path[i]));
            if(! current.containsKey(path[i]) || ! (current.get(path[i]) instanceof Map) ) {
                current.put(path[i], new HashMap<String, Object>());
            }
            current = (Map<String, Object>) current.get(path[i]);
        }
        // Now we can simply put the value
        return current.put(path[path.length - 1], value);
    }

    @Override
    @Deprecated
    public void putAll(Map<? extends String, ? extends Object> m) {
        throw new UnsupportedOperationException("don't call put from a processor");
    }

}
