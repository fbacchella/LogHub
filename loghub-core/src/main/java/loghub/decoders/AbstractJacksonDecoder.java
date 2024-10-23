package loghub.decoders;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.jackson.JacksonBuilder;

public abstract class AbstractJacksonDecoder<JB extends AbstractJacksonDecoder.Builder<? extends AbstractJacksonDecoder<JB, OM>>, OM extends ObjectMapper> extends Decoder {

    public abstract static class Builder<B extends AbstractJacksonDecoder<?, ?>> extends Decoder.Builder<B> {
    }

    @FunctionalInterface
    public interface ObjectResolver {
        Object deserialize(ObjectReader reader) throws IOException;
    }

    private final ObjectReader reader;

    protected AbstractJacksonDecoder(JB builder) {
        super(builder);
        this.reader = getReaderBuilder(builder).getReader();
    }

    protected abstract JacksonBuilder<OM> getReaderBuilder(JB builder);

   @Override
    public Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return runDecodeJackson(ctx, reader -> reader.readValue(msg, offset, length));
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return runDecodeJackson(ctx, reader -> reader.readValue((InputStream)new ByteBufInputStream(bbuf)));
    }

    protected final Object runDecodeJackson(ConnectionContext<?> ctx, ObjectResolver gen) throws DecodeException {
        try {
            return decodeJackson(ctx, gen);
        } catch (IOException ex) {
            throw new DecodeException("Failed reading message content: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
            throws DecodeException, IOException {
        return gen.deserialize(reader);
    }

}
