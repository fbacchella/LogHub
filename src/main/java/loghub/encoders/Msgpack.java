package loghub.encoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.msgpack.jackson.dataformat.MessagePackExtensionType;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.jackson.dataformat.MessagePackGenerator;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import lombok.Setter;

@BuilderClass(Msgpack.Builder.class)
@CanBatch
public class Msgpack extends Encoder {

    public static class Builder extends Encoder.Builder<Msgpack> {
        @Setter
        private boolean forwardEvent = false;
        @Override
        public Msgpack build() {
            return new Msgpack(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private static class EventSerializer extends JsonSerializer<Event> {
        @Override
        public void serialize(Event value, JsonGenerator gen,
                              SerializerProvider serializers)
                                              throws IOException {
            MessagePackGenerator generator = (MessagePackGenerator) gen;
            Map<String, Object> eventContent = new HashMap<>();
            Map<String, Object> eventFields = new HashMap<>();
            Map<String, Object> eventMetas = new HashMap<>();
            eventFields.putAll(value);
            value.getMetaAsStream().forEach( i-> eventMetas.put(i.getKey(), i.getValue()));
            eventContent.put("@fields", eventFields);
            eventContent.put(Event.TIMESTAMPKEY, value.getTimestamp());
            eventContent.put("@METAS", eventMetas);
            Map<String, Object> eventMap = Collections.singletonMap(Event.class.getCanonicalName(), eventContent);
            generator.writeObject(eventMap);
        }
        @Override
        public Class<Event> handledType() {
            return Event.class;
        }
    }

    private static abstract class TimeSerializer<K> extends JsonSerializer<K> {
        void doSerialiaze(long seconds, int nanoseconds, MessagePackGenerator gen) throws IOException {
            ByteBuffer longBuffer = ByteBuffer.wrap(new byte[12]);
            longBuffer.order(ByteOrder.BIG_ENDIAN);
            long result = ((long)nanoseconds << 34) | seconds;
            int size = 0;
            if ((result >> 34) == 0) {
                if ((result & 0xffffffff00000000L) == 0 ) {
                    longBuffer.putInt((int) result);
                    size = 4;
                } else {
                    longBuffer.putLong(result);
                    size = 8;
                }
            } else {
                longBuffer.putInt(nanoseconds);
                longBuffer.putLong(seconds);
                size = 12;
            }
            MessagePackExtensionType ext = new MessagePackExtensionType((byte)-1, Arrays.copyOf(longBuffer.array(), size));
            gen.writeExtensionType(ext);
        }
    }

    private static class DateSerializer extends TimeSerializer<Date> {
        @Override
        public void serialize(Date value, JsonGenerator gen,
                              SerializerProvider serializers)
                                              throws IOException {
            long seconds = Math.floorDiv(value.getTime(), 1000L);
            int nanoseconds = ((int)(value.getTime() % 1000L)) * 1000000;
            doSerialiaze(seconds, nanoseconds, (MessagePackGenerator) gen);
        }

        @Override
        public Class<Date> handledType() {
            return Date.class;
        }
    }

    private static class InstantSerializer extends TimeSerializer<Instant> {
        @Override
        public void serialize(Instant value, JsonGenerator gen,
                              SerializerProvider serializers)
                                              throws IOException {
            long seconds = value.getEpochSecond();
            int nanoseconds = value.getNano();
            doSerialiaze(seconds, nanoseconds, (MessagePackGenerator) gen);
        }

        @Override
        public Class<Instant> handledType() {
            return Instant.class;
        }
    }

    private static final JsonFactory factory = new MessagePackFactory();
    private static final ThreadLocal<ObjectMapper> msgpackAsEvent;
    private static final ThreadLocal<ObjectMapper> msgpackAsMap;
    static {
        // The are shared by ObjectMapper in a unknown way, don't create useless instance.
        DateSerializer ds = new DateSerializer();
        InstantSerializer is = new InstantSerializer();
        EventSerializer es = new EventSerializer();
        msgpackAsEvent = ThreadLocal.withInitial(() ->  {
            ObjectMapper mapper = new ObjectMapper(factory);
            SimpleModule dateModule = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "MsgpackAsEvent"));
            dateModule.addSerializer(ds);
            dateModule.addSerializer(is);
            dateModule.addSerializer(es);
            mapper.registerModule(dateModule);
            return mapper;
        });
        msgpackAsMap = ThreadLocal.withInitial(() ->  {
            ObjectMapper mapper = new ObjectMapper(factory);
            SimpleModule dateModule = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "MsgpackAsMap"));
            dateModule.addSerializer(ds);
            dateModule.addSerializer(is);
            mapper.registerModule(dateModule);
            return mapper;
        });
    }

    private final ThreadLocal<ObjectMapper> mapper;

    private Msgpack(Builder builder) {
        super(builder);
        mapper = builder.forwardEvent ? msgpackAsEvent : msgpackAsMap;
    }

    @Override
    public byte[] encode(Event event) {
        try {
            return mapper.get().writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] encode(Stream<Event> events) {
        try {
            return mapper.get().writeValueAsBytes(events.collect(Collectors.toList()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
