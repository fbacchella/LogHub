package loghub.decoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Event;

/**
 * This transformer parse a msgpack object. If it's a map, all the elements are
 * added to the event. Otherwise it's content is added to the field indicated.
 * 
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(Msgpack.Builder.class)
public class Msgpack extends Decoder {

    public static class Builder extends Decoder.Builder<Msgpack> {
        @Override
        public Msgpack build() {
            return new Msgpack(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { };

    private static class TimeDeserializer implements ExtensionTypeCustomDeserializers.Deser {
        @Override
        public Object deserialize(byte[] data)
                        throws IOException
        {
            ByteBuffer content = ByteBuffer.wrap(data);
            long seconds = 0;
            int nanoseconds = 0;
            boolean found = false;
            switch (data.length) {
            case 4:
                seconds = content.getInt();
                nanoseconds = 0;
                found = true;
                break;
            case 8:
                long lcontent = content.getLong();
                seconds = lcontent & 0x00000003ffffffffL;
                // Masked needed to drop sign extended by right shift
                nanoseconds = (int)((lcontent >> 34) & (0x3FFFFFFFL));
                found = true;
                break;
            case 12:
                nanoseconds = content.getInt();
                seconds = content.getLong();
                found = true;
                break;
            default:
                throw new IOException("Invalid time object length");
            }
            if (found) {
                try {
                    return Instant.ofEpochSecond(seconds, nanoseconds);
                } catch (DateTimeException e) {
                    return data;
                }
            } else {
                return data;
            }
        }
    }
    private static final ExtensionTypeCustomDeserializers extTypeCustomDesers = new ExtensionTypeCustomDeserializers();
    static {
        extTypeCustomDesers.addCustomDeser((byte) -1, new TimeDeserializer());
    }
    private static final JsonFactory factory = new MessagePackFactory().setExtTypeCustomDesers(extTypeCustomDesers);
    private static final ThreadLocal<ObjectMapper> msgpack = ThreadLocal.withInitial(() ->  {
        return new ObjectMapper(factory);
    });

    private Msgpack(Builder builder) {
        super(builder);
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        try {
            Object o = msgpack.get().readValue(msg, offset, length, Object.class);
            return decodeValue(ctx, o);
        } catch (MessageInsufficientBufferException e) {
            throw new DecodeException("Reception buffer too small");
        } catch (MessagePackException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        } catch (IOException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        }
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        try {
            Object o = msgpack.get().readValue(new ByteBufInputStream(bbuf), OBJECTREF);
            return decodeValue(ctx, o);
        } catch (MessageInsufficientBufferException e) {
            throw new DecodeException("Reception buffer too small");
        } catch (MessagePackException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        } catch (IOException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        }
    }

    private Map<String, Object> decodeValue(ConnectionContext<?> ctx, Object o) throws DecodeException {
        try {
            if(o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                if (map.size() == 1 && map.containsKey(Event.class.getCanonicalName())) {
                    // Special case, the message contain a loghub event, sent from another loghub
                    @SuppressWarnings("unchecked")
                    Map<String, Object> eventContent = (Map<String, Object>) map.remove(Event.class.getCanonicalName());
                    @SuppressWarnings("unchecked")
                    Map<String, Object> fields = (Map<String, Object>)eventContent.remove("@fields");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> metas = (Map<String, Object>) eventContent.remove("@METAS");
                    Event newEvent = Event.emptyEvent(ctx);
                    newEvent.putAll(fields);
                    Optional.ofNullable(eventContent.get(Event.TIMESTAMPKEY))
                    .filter(newEvent::setTimestamp)
                    .ifPresent(ts -> eventContent.remove(Event.TIMESTAMPKEY));
                    metas.forEach((i,j) -> newEvent.putMeta(i, j));
                    return newEvent;
                } else {
                    Map<String, Object> newMap = new HashMap<>(map.size());
                    map.entrySet().stream().forEach((i) -> newMap.put(i.getKey().toString(), i.getValue()));
                    return newMap;
                }
            } else {
                return Collections.singletonMap(field, o);
            }
        } catch (MessageInsufficientBufferException e) {
            throw new DecodeException("Reception buffer too small");
        } catch (MessagePackException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        }
    }

    public String getField() {
        return field;
    }
}
