package loghub.decoders;

import java.io.IOException;

import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.databind.ObjectReader;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.jackson.JacksonBuilder;
import loghub.jackson.MsgpackTimeDeserializer;

/**
 * This transformer parse a msgpack object. If it's a map, all the elements are
 * added to the event. Otherwise it's content is added to the field indicated.
 * 
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(Msgpack.Builder.class)
public class Msgpack extends AbstractJackson {

    public static class Builder extends AbstractJackson.Builder<Msgpack> {
        @Override
        public Msgpack build() {
            return new Msgpack(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final ObjectReader reader;
    static {
        ExtensionTypeCustomDeserializers extTypeCustomDesers = new ExtensionTypeCustomDeserializers();
        extTypeCustomDesers.addCustomDeser((byte) -1, new MsgpackTimeDeserializer());
        reader = JacksonBuilder.get()
                .setFactory(new MessagePackFactory().setExtTypeCustomDesers(extTypeCustomDesers))
                .getReader();
    }

    private Msgpack(Builder builder) {
        super(builder);
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
            throws DecodeException, IOException {
        try {
            return gen.deserialize(reader);
        } catch (MessagePackException ex) {
            throw new DecodeException("Failed to decode Msgpack event: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

}
