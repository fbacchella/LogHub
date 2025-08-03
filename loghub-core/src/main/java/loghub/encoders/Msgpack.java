package loghub.encoders;

import org.msgpack.jackson.dataformat.MessagePackMapper;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.jackson.EventSerializer;
import loghub.jackson.JacksonBuilder;
import loghub.jackson.MsgpackTimeSerializer.DateSerializer;
import loghub.jackson.MsgpackTimeSerializer.InstantSerializer;
import loghub.types.MimeType;
import lombok.Setter;

@BuilderClass(Msgpack.Builder.class)
@CanBatch
public class Msgpack extends AbstractJacksonEncoder<Msgpack.Builder, MessagePackMapper> {

    public static final MimeType MIME_TYPE = MimeType.of("application/vnd.msgpack");

    @Setter
    public static class Builder extends AbstractJacksonEncoder.Builder<Msgpack> {
        public boolean forwardEvent = false;
        @Override
        public Msgpack build() {
            return new Msgpack(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final SimpleModule dateModuleEvent;
    private static final SimpleModule dateModuleMap;
    static {
        // They are shared by ObjectMapper, don't create useless instances.
        DateSerializer ds = new DateSerializer();
        InstantSerializer is = new InstantSerializer();
        EventSerializer es = new EventSerializer();
        dateModuleEvent = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "MsgpackAsEvent"));
        dateModuleEvent.addSerializer(ds);
        dateModuleEvent.addSerializer(is);
        dateModuleEvent.addSerializer(es);
        dateModuleMap = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "MsgpackAsMap"));
        dateModuleMap.addSerializer(ds);
        dateModuleMap.addSerializer(is);
    }

    private Msgpack(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<MessagePackMapper> getWriterBuilder(Builder builder) {
        return JacksonBuilder.get(MessagePackMapper.class)
                             .module(builder.forwardEvent ? dateModuleEvent : dateModuleMap);
    }

    @Override
    public MimeType getMimeType() {
        return MIME_TYPE;
    }

}
