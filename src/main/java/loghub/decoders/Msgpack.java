package loghub.decoders;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import loghub.Decoder;
import loghub.configuration.Beans;

/**
 * This transformer parse a msgpack object. If it's a map, all the elements are
 * added to the event. Otherwise it's content is added to the field indicated.
 * 
 * @author Fabrice Bacchella
 *
 */
@Beans("field")
public class Msgpack extends Decoder {

    private static final JsonFactory factory = new MessagePackFactory();
    private static final ThreadLocal<ObjectMapper> msgpack = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper(factory);
        }
    };

    private String field = "message";

    @Override
    public Map<String, Object> decode(byte[] msg, int offset, int length) {
        try {
            Object o = msgpack.get().readValue(msg, offset, length, Object.class);
            if(o instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>) o;
                final Map<String, Object> newMap = new HashMap<>(map.size());
                map.entrySet().stream().forEach((i) -> newMap.put(i.getKey().toString(), i.getValue()));
                return newMap;
            } else {
                return Collections.singletonMap(field, o);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
