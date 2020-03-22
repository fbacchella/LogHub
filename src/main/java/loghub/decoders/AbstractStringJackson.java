package loghub.decoders;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import lombok.Setter;

public abstract class AbstractStringJackson extends AbstractJackson {


    public abstract static class Builder<B extends AbstractStringJackson> extends AbstractJackson.Builder<B> {
        @Setter
        private String charset = Charset.defaultCharset().name();
    };

    private final Charset charset;

    protected AbstractStringJackson(Builder<? extends AbstractStringJackson> builder) {
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

}
