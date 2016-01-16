package loghub;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Event extends HashMap<String, Object> implements Serializable {

    private final static Logger logger = LogManager.getLogger();

    public static final String TIMESTAMPKEY = "__timestamp";
    public static final String TYPEKEY = "__type";

    public Date timestamp;
    public String type;
    public boolean dropped = false;

    public Event() {
        super();
        timestamp = new Date();
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

    @Override
    public String toString() {
        return type + "[" + timestamp + "]" + super.toString();
    }

    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value) {
        return applyAtPath(f, path, value, false);
    }

    public Object applyAtPath(Helpers.TriFunction<Map<String, Object>, String, Object, Object> f, String[] path, Object value, boolean create) {
        Map<String, Object> current = this;
        String key = path[0];
        for(int i = 0; i < path.length - 1; i++) {
            @SuppressWarnings("unchecked")
            Map<String, Object> next = (Map<String, Object>) current.get(key);
            if( next == null || ! (next instanceof Map) ) {
                if(create) {
                    next = new HashMap<String, Object>();
                    current.put(path[i], next);
                } else {
                    return null;
                }
            }
            current = next;
            key = path[i + 1];
        }
        return f.apply(current, key, value);
    }

}
