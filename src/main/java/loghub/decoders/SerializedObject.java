package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import lombok.Setter;

@BuilderClass(SerializedObject.Builder.class)
public class SerializedObject extends Decoder {

    public static class Builder extends Decoder.Builder<SerializedObject> {
        @Setter
        private String objectFied = "objectClass";
        @Override
        public SerializedObject build() {
            return new SerializedObject(this);
        }

    };

    private final String objectFied;

    private SerializedObject(Builder builder) {
        super(builder);
        objectFied = builder.objectFied;
    }

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

}
