package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Decoder;
import loghub.configuration.Beans;

@Beans({"field", "objectFied"})
public class SerializedObject extends Decoder {

    private static final Logger logger = LogManager.getLogger();

    private String field = "message";
    private String objectFied = "objectClass";

    @Override
    public Map<String, Object> decode(byte[] msg, int offset, int length) {
        Map<String, Object> map = new HashMap<>();
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(msg, offset, length))) {
            Object o = ois.readObject();
            map.put(field, o);
            if ( objectFied != null && !objectFied.isEmpty()) {
                map.put(objectFied, o.getClass().getCanonicalName());
            }
        } catch (IOException e) {
            logger.error("IO exception while reading ByteArrayInputStream: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
        } catch (ClassNotFoundException e) {
            logger.error("Unable to unserialize log4j event: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
        }
        return map;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return the objectFied
     */
    public String getObjectFied() {
        return objectFied;
    }

    /**
     * @param objectFied the objectFied to set
     */
    public void setObjectFied(String objectFied) {
        this.objectFied = objectFied;
    }

}
