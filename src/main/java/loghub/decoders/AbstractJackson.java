package loghub.decoders;

import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.ConnectionContext;

public abstract class AbstractJackson extends Decoder {

    protected static final TypeReference<Object> OBJECTREF = new TypeReference<Object>() { };

    protected AbstractJackson(Builder<? extends AbstractJackson> builder) {
        super(builder);
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) {
        throw new UnsupportedOperationException();
    }
    @Override
    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return decodeStreamJackson(ctx, om -> om.readValue(msg, offset, length, OBJECTREF));
    }

    @Override
    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return decodeStreamJackson(ctx, om -> om.readValue(new ByteBufInputStream(bbuf), OBJECTREF));
    }

    public Stream<Map<String, Object>> decodeStream(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        return decodeStream(ctx, msg, 0, msg.length);
    }

    protected abstract Stream<Map<String, Object>> decodeStreamJackson(ConnectionContext<?> ctx, JacksonDeserializer.ObjectResolver gen) throws DecodeException;

}
