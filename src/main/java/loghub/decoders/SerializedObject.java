package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.configuration.Beans;

@Beans({"field", "objectFied"})
public class SerializedObject extends Decoder {

    private String field = "message";
    private String objectFied = "objectClass";

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(msg, offset, length))) {
            return decodeStream(ois);
        } catch (IOException e) {
            throw new DecodeException("IO exception while reading ByteArrayInputStream: " + e.getMessage(), e);
        }
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteBufInputStream(bbuf))) {
            return decodeStream(ois);
        } catch (IOException e) {
            throw new DecodeException("IO exception while reading ByteArrayInputStream: " + e.getMessage(), e);
        }
    }

    private Map<String, Object> decodeStream(ObjectInputStream ois) throws DecodeException {
        Map<String, Object> map = new HashMap<>();
        try {
            Object o = ois.readObject();
            map.put(field, o);
            if ( objectFied != null && !objectFied.isEmpty()) {
                map.put(objectFied, o.getClass().getCanonicalName());
            }
        } catch (IOException e) {
            throw new DecodeException("IO exception while reading ByteArrayInputStream: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            throw new DecodeException("Unable to unserialize log4j event: " + e.getMessage(), e);
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
