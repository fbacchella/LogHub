package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Event extends HashMap<String, Object> implements Serializable {

    private final static Logger logger = LogManager.getLogger();
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
        key = Arrays.copyOf(getNewKey(), 8);
    }

    /**
     * Return a deep copy of the event.
     * 
     * It work by doing serialize/deserialize ofthe event. So a event must
     * only contains serializable object to make it works.
     * 
     * @return a copy of this event, with a different key
     */
    public Event duplicate() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(this);
            oos.flush();
            oos.close();
            bos.close();
            byte[] byteData = bos.toByteArray();
            ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
            Event newEvent = (Event) new ObjectInputStream(bais).readObject();

            //Generate a new key, needed because the byte[] key is reloaded
            byte[] newKeyBuffer = getNewKey();
            for(int i = 0; i < newKeyBuffer.length; i++) {
                key[i] = newKeyBuffer[i];
            }
            return newEvent;
        } catch (NotSerializableException ex) {
            logger.info("Event copy failed: {}", ex.getMessage());
            logger.catching(Level.DEBUG, ex);
            return null;
        } catch (ClassNotFoundException | IOException ex) {
            logger.fatal("Event copy failed: {}", ex.getMessage());
            logger.catching(Level.FATAL, ex);
            return null;
        }
    }

    private byte[] getNewKey() {
        long keyValue = KeyGenerator.getAndIncrement();
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(keyValue);
        return buffer.array();
    }

    @Override
    public String toString() {
        return type + "[" + timestamp + "]" + super.toString();
    }

    public byte[] key() {
        return key;
    }

    @SuppressWarnings("unchecked")
    public Object put(String[] path, Object value) {
        Map<String, Object> current = this;
        String key = path[0];
        for(int i = 0; i < path.length - 1; i++) {
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if(next == null || ! (next instanceof Map) ) {
                next = new HashMap<String, Object>();
                current.put(path[i], next);
            }
            current = next;
            key = path[i + 1];
        }
        // Now we can simply put the value
        return current.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public Object get(String[] path) {
        Map<String, Object> current = this;
        String key = path[0];
        for(int i = 0; i < path.length - 1; i++) {
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if( next == null || ! (next instanceof Map) ) {
                return null;
            }
            current = next;
            key = path[i + 1];
        }
        return current.get(key);
    }

    @SuppressWarnings("unchecked")
    public boolean containsKey(String[] path) {
        Map<String, Object> current = this;
        String key = path[0];
        for(int i = 0; i < path.length - 1; i++) {
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if( next == null || ! (next instanceof Map) ) {
                return false;
            }
            current = next;
            key = path[i + 1];
        }
        return current.containsKey(key);
    }

    @SuppressWarnings("unchecked")
    public Object remove(String[] path) {
        Map<String, Object> current = this;
        String key = path[0];
        for(int i = 0; i < path.length - 1; i++) {
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if( next == null || ! (next instanceof Map) ) {
                return null;
            }
            current = next;
            key = path[i + 1];
        }
        return current.remove(key);
    }
}
