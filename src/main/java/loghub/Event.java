package loghub;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Event extends HashMap<String, Object> implements Serializable {
    
    private final static AtomicLong KeyGenerator = new AtomicLong(0);

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
    
}
