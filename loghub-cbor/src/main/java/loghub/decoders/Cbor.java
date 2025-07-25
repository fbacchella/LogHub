package loghub.decoders;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.cbor.CborParser;
import lombok.Setter;

@BuilderClass(Cbor.Builder.class)
public class Cbor extends Decoder {

    @Setter
    public static class Builder extends Decoder.Builder<Cbor> {
        @Override
        public Cbor build() {
            return new Cbor(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private Cbor(Builder builder) {
        super(builder);
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length)
            throws DecodeException {
        try {
            return CborParser.parse(msg, offset, length);
        } catch (IOException e) {
            throw new DecodeException("Unable to read CBOR buffer", e);
        }
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        try {
            return CborParser.parse(new ByteBufInputStream(bbuf));
        } catch (IOException e) {
            throw new DecodeException("Unable to read CBOR buffer", e);
        }
    }

}
