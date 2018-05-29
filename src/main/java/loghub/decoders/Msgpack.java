package loghub.decoders;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;
import loghub.Decoder;

/**
 * This transformer parse a msgpack object. If it's a map, all the elements are
 * added to the event. Otherwise it's content is added to the field indicated.
 * 
 * @author Fabrice Bacchella
 *
 */
public class Msgpack extends Decoder {

    private static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { };

    private static final JsonFactory factory = new MessagePackFactory();
    private static final ThreadLocal<ObjectMapper> msgpack = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    private String field = "message";

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        try {
            Object o = msgpack.get().readValue(msg, offset, length, Object.class);
            return decodeValue(o);
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
            return decodeValue(o);
        } catch (MessageInsufficientBufferException e) {
            throw new DecodeException("Reception buffer too small");
        } catch (MessagePackException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        } catch (IOException e) {
            throw new DecodeException("Can't parse msgpack serialization", e);
        }
    }

    private Map<String, Object> decodeValue(Object o) throws DecodeException {
        try {
            if(o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                Map<String, Object> newMap = new HashMap<>(map.size());
                map.entrySet().stream().forEach((i) -> newMap.put(i.getKey().toString(), i.getValue()));
                return newMap;
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

    public void setField(String field) {
        this.field = field;
    }
}
