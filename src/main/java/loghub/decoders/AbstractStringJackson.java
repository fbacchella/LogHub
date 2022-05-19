package loghub.decoders;

import java.nio.charset.Charset;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import lombok.Setter;

public abstract class AbstractStringJackson<JB extends AbstractStringJackson.Builder<? extends AbstractStringJackson<JB>>> extends AbstractJacksonDecoder<JB> implements TextDecoder {

    public abstract static class Builder<B extends AbstractStringJackson<?>> extends AbstractJacksonDecoder.Builder<B> {
        @Setter
        protected String charset = Charset.defaultCharset().name();
    }

    private final Charset charset;

    protected AbstractStringJackson(JB builder) {
        super(builder);
        charset = Charset.forName(builder.charset);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return runDecodeJackson(ctx, reader -> reader.readValues(new String(msg, offset, length, charset)));
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return runDecodeJackson(ctx, reader -> reader.readValues(bbuf.toString(charset)));
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, String message)
                    throws DecodeException {
        return runDecodeJackson(ctx, reader -> reader.readValues(message));
    }

    @Override
    public Stream<Map<String, Object>> decode(ConnectionContext<?> ctx,
                                              String message)
                    throws DecodeException {
        return parseObjectStream(ctx, () -> decodeObject(ctx, message));
    }

    @Override
    public String getCharset() {
        return charset.displayName(Locale.ENGLISH);
    }

}
