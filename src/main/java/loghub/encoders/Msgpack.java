package loghub.encoders;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Event;
import loghub.jackson.EventSerializer;
import loghub.jackson.MsgpackTimeSerializer.DateSerializer;
import loghub.jackson.MsgpackTimeSerializer.InstantSerializer;
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
    public byte[] encode(Event event) throws EncodeException {
        try {
            return mapper.get().writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode to MsgPack", e);
        }
    }

    @Override
    public byte[] encode(Stream<Event> events) throws EncodeException {
        try {
            return mapper.get().writeValueAsBytes(events.collect(Collectors.toList()));
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode to MsgPack", e);
        }
    }

}
