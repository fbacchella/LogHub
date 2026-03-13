package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.GeneratedMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.grpc.BinaryCodec;
import loghub.grpc.GrpcStreamHandler;
import lombok.Setter;

@BuilderClass(ProtoBuf.Builder.class)
public class ProtoBuf extends Decoder {

    private interface GetInputStream {
        InputStream get();
    }

    @Setter
    public static class Builder extends Decoder.Builder<ProtoBuf> {
        protected String schemaUri;
        protected String mappingClass;
        protected ClassLoader loader;
        protected String[] knowMessages = new String[] {};

        @Override
        public ProtoBuf build() {
            return new ProtoBuf(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    protected final BinaryCodec<GrpcStreamHandler> decoder;
    protected final String mappingClass;

    protected ProtoBuf(Builder builder) {
        super(builder);
        mappingClass = builder.mappingClass;
        try {
            decoder = getDecoder(builder);
        } catch (Descriptors.DescriptorValidationException | IOException ex) {
            throw new IllegalArgumentException("Unusable binary schema:" + Helpers.resolveThrowableException(ex), ex);
        }
        if (builder.loader != null && builder.knowMessages != null) {
            for (String clazz : builder.knowMessages) {
                try {
                    Class<?> loadedClass = builder.loader.loadClass(clazz);
                    if (GeneratedMessage.class.isAssignableFrom(loadedClass)) {
                        Method builderMethod = loadedClass.getMethod("parseFrom", CodedInputStream.class);
                        decoder.addFastPath(clazz,
                                (BinaryCodec.MessageFastPathFunction<? extends Object>) (s, d, u) -> parseFrom(
                                        builderMethod, s));
                    }
                } catch (ClassNotFoundException | NoSuchMethodException ex) {
                    throw new IllegalArgumentException("Unusable helpers class:" + Helpers.resolveThrowableException(ex), ex);
                }
            }
        }
    }

    protected BinaryCodec<GrpcStreamHandler> getDecoder(Builder builder) throws DescriptorValidationException, IOException {
        return new BinaryCodec<>(Helpers.fileUri(builder.schemaUri));
    }

    private Object parseFrom(Method builderMethod, CodedInputStream stream) {
        try {
            return builderMethod.invoke(null, stream);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new IllegalArgumentException("Unusable helpers class:" + Helpers.resolveThrowableException(ex), ex);
        }
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        return parse(() -> new ByteBufInputStream(bbuf));
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg, int offset, int length) throws DecodeException {
        return parse(() -> new ByteArrayInputStream(msg, offset, length));
    }

    private Object parse(GetInputStream getis) throws DecodeException {
        try (InputStream is = getis.get()) {
            List<BinaryCodec.UnknownField> unknownFields = new ArrayList<>();
            return decoder.decode(CodedInputStream.newInstance(is), mappingClass, unknownFields);
        } catch (IOException ex) {
            throw new DecodeException("Failed to decode Protobuf event: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

    public BinaryCodec<GrpcStreamHandler> getProtobufCodec() {
        return decoder;
    }

}
