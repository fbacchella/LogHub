package loghub.decoders;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import lombok.Setter;

@BuilderClass(StringCodec.Builder.class)
public class StringCodec extends Decoder {

    public static class Builder extends Decoder.Builder<StringCodec> {
        @Setter
        private String charset = Charset.defaultCharset().name();
        @Override
        public StringCodec build() {
            return new StringCodec(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Charset charset;

    private StringCodec(Builder builder) {
        super(builder);
        charset = Charset.forName(builder.charset);
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) {
        String message = new String(msg, offset, length, charset);
        return Collections.singletonMap(field, message);
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) {
        return Collections.singletonMap(field, bbuf.toString(charset));
    }

}
