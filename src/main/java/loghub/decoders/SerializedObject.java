package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;

@BuilderClass(SerializedObject.Builder.class)
public class SerializedObject extends Decoder {

    public static class Builder extends Decoder.Builder<SerializedObject> {
        @Override
        public SerializedObject build() {
            return new SerializedObject(this);
        }

    };

    private SerializedObject(Builder builder) {
        super(builder);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return deserialize(() -> {
            try {
                return new ObjectInputStream(new ByteArrayInputStream(msg, offset, length));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return deserialize(() -> {
            try {
                return new ObjectInputStream(new ByteBufInputStream(bbuf));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private Object deserialize(Supplier<ObjectInputStream> oisp) throws DecodeException {
        try (ObjectInputStream ois = oisp.get()){
            return ois.readObject();
        } catch (UncheckedIOException e) {
            throw new DecodeException("IO exception while reading ByteArrayInputStream: " + Helpers.resolveThrowableException(e), e.getCause());
        } catch (IOException e) {
            throw new DecodeException("IO exception while reading ByteArrayInputStream: " + Helpers.resolveThrowableException(e), e);
        } catch (ClassNotFoundException e) {
            throw new DecodeException("Unable to unserialize log4j event: " + Helpers.resolveThrowableException(e), e);
        }
    }

}
