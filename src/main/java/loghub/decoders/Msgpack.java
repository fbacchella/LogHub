package loghub.decoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.msgpack.core.MessageInsufficientBufferException;
import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.Event;

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
    private static final  ThreadLocal<ObjectMapper> msgpack = ThreadLocal.withInitial(() ->  new ObjectMapper(factory));

    private String field = "message";
    private String dateField = "timestamp";

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
                map.forEach((i, j) -> {
                    if (j instanceof MessagePackExtensionType) {
                        MessagePackExtensionType ext = (MessagePackExtensionType) j;
                        // It's a time stamp, convert it
                        if (ext.getType() == -1) {
                            ByteBuffer content = ByteBuffer.wrap(ext.getData());
                            Date timestamp;
                            long seconds = 0;
                            int nanoseconds = 0;
                            boolean found = false;
                            switch (ext.getData().length) {
                            case 4:
                                seconds = content.getInt();
                                nanoseconds = 0;
                                found = true;
                                break;
                            case 8:
                                long lcontent = content.getLong();
                                nanoseconds = (int) (lcontent >> 34);
                                seconds = lcontent & 0x00000003ffffffffL;
                                found = true;
                                break;
                            case 12:
                                nanoseconds = content.getInt();
                                seconds = content.getLong();
                                found = true;
                                break;
                            }
                            if (found) {
                                try {
                                    timestamp = Date.from(Instant.ofEpochSecond(seconds, nanoseconds));
                                    map.put(i,timestamp);
                                } catch (DateTimeException e) {
                                }
                            }
                        }
                    }
                });
                // Check how the timestamp was parsed
                if (map.containsKey(Event.TIMESTAMPKEY)) {
                    Object ts = map.get(Event.TIMESTAMPKEY);
                    // It was not resolved as a date, needs to be checked with more details
                    if (! (ts instanceof Date)) {
                        map.remove(Event.TIMESTAMPKEY);
                        if (dateField != null) {
                            map.put(dateField, ts);
                        }
                    }
                }
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

    public String getDateField() {
        return dateField;
    }

    public void setDateField(String dateField) {
        this.dateField = dateField;
    }

}
