package loghub.decoders;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;

public abstract class AbstractJackson extends Decoder {

    public abstract static class Builder<B extends AbstractJackson> extends Decoder.Builder<B> {
    };

    @FunctionalInterface
    public static interface ObjectResolver {
        Object deserialize(ObjectReader reader) throws DecodeException, IOException;
    }

    protected static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { };

    protected AbstractJackson(Builder<? extends AbstractJackson> builder) {
        super(builder);
    }

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
            throw new DecodeException("Failed reading message content: " + ex.getMessage(), ex);
        }
    }

    protected abstract Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen) throws DecodeException, IOException;

}
