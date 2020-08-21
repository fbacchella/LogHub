package loghub.decoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.DateTimeException;
import java.time.Instant;

import org.msgpack.core.MessagePackException;
import org.msgpack.jackson.dataformat.ExtensionTypeCustomDeserializers;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;

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

    private static final ObjectReader reader;
    static {
        ExtensionTypeCustomDeserializers extTypeCustomDesers = new ExtensionTypeCustomDeserializers();
        extTypeCustomDesers.addCustomDeser((byte) -1, new TimeDeserializer());
        JsonFactory factory = new MessagePackFactory().setExtTypeCustomDesers(extTypeCustomDesers);
        ObjectMapper mapper = new ObjectMapper(factory);
        reader = mapper.readerFor(OBJECTREF);
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
