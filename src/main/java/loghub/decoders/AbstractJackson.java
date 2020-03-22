package loghub.decoders;

import java.io.IOException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;

public abstract class AbstractJackson extends Decoder {

    public abstract static class Builder<B extends AbstractJackson> extends Decoder.Builder<B> {
    };

    @FunctionalInterface
    public static interface ObjectResolver {
        Object deserialize(ObjectMapper om) throws DecodeException, IOException;
    }

    protected static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { };

    protected AbstractJackson(Builder<? extends AbstractJackson> builder) {
        super(builder);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return runDecodeJackson(ctx, om -> om.readValue(msg, offset, length, OBJECTREF));
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return runDecodeJackson(ctx, om -> om.readValue(new ByteBufInputStream(bbuf), OBJECTREF));
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
