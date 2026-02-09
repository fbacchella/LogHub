package loghub.decoders;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.PojoConverter;
import lombok.Setter;

@BuilderClass(KaitaiDecoder.Builder.class)
public class KaitaiDecoder extends Decoder {

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    @Setter
    public static class Builder extends Decoder.Builder<KaitaiDecoder> {
        private String kaitaiStruct;
        @Override
        public KaitaiDecoder build() {
            return new KaitaiDecoder(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final PojoConverter converter = new PojoConverter();
    private final MethodHandle kaitaiStructConstructor;

    private KaitaiDecoder(Builder builder) {
        super(builder);
        kaitaiStructConstructor = resolveConstructor(builder.kaitaiStruct);
    }

    private MethodHandle resolveConstructor(String kaitaiStruct) {
        try {
            Class<? extends KaitaiStruct> structClass = (Class<? extends KaitaiStruct>) Class.forName(kaitaiStruct);
            if (!KaitaiStruct.class.isAssignableFrom(structClass)) {
                throw new IllegalArgumentException(
                        "Class " + structClass.getName() + " does not inherit from KaitaiStruct"
                );
            }
            MethodType constructorType = MethodType.methodType(void.class, KaitaiStream.class);
            return LOOKUP.findConstructor(structClass, constructorType);
        } catch (ClassNotFoundException | IllegalArgumentException | NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        KaitaiStream stream = new ByteBufferKaitaiStream(bbuf.nioBuffer());
        return decodeKaitai(stream);
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuffer bbuf) throws DecodeException {
        KaitaiStream stream = new ByteBufferKaitaiStream(bbuf);
        return decodeKaitai(stream);
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        KaitaiStream stream = new ByteBufferKaitaiStream(msg);
        return decodeKaitai(stream);
    }

    private Object decodeKaitai(KaitaiStream stream) throws DecodeException {
        try {
            KaitaiStruct struct = (KaitaiStruct) kaitaiStructConstructor.invoke(stream);
            return converter.pojoToMap(struct);
        } catch (Throwable e) {
            throw new DecodeException("", e);
        }
    }

}
