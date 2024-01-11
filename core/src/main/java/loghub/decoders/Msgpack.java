package loghub.decoders;

import java.io.IOException;

import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.jackson.dataformat.MessagePackMapper;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.jackson.JacksonBuilder;
import loghub.jackson.MsgpackTimeDeserializer;

/**
 * This transformer parse a msgpack object. If it's a map, all the elements are
 * added to the event. Otherwise, it's content is added to the field indicated.
 * 
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(Msgpack.Builder.class)
public class Msgpack extends AbstractJacksonDecoder<Msgpack.Builder, MessagePackMapper> {

    public static class Builder extends AbstractJacksonDecoder.Builder<Msgpack> {
        @Override
        public Msgpack build() {
            return new Msgpack(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final MessagePackFactory factory;
    static {
        ExtensionTypeCustomDeserializers extTypeCustomDesers = new ExtensionTypeCustomDeserializers();
        extTypeCustomDesers.addCustomDeser((byte) -1, new MsgpackTimeDeserializer());
        factory = new MessagePackFactory().setExtTypeCustomDesers(extTypeCustomDesers);
    }

    private Msgpack(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<MessagePackMapper> getReaderBuilder(Builder builder) {
        return JacksonBuilder.get(MessagePackMapper.class, factory);
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
            throws DecodeException, IOException {
        try {
            return super.decodeJackson(ctx, gen);
        } catch (MessagePackException ex) {
            throw new DecodeException("Failed to decode Msgpack event: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

}
