package loghub.decoders;

import java.util.Arrays;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;

@BuilderClass(Raw.Builder.class)
public class Raw extends Decoder {

    public static class Builder extends Decoder.Builder<Raw> {
        @Override
        public Raw build() {
            return new Raw(this);
        }
    }
    public static Raw.Builder getBuilder() {
        return new Raw.Builder();
    }

    private Raw(Raw.Builder builder) {
        super(builder);
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        byte[] content =  new byte[bbuf.readableBytes()];
        bbuf.readBytes(content);
        return content;
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return Arrays.copyOfRange(msg,offset, offset + length);
    }

}
