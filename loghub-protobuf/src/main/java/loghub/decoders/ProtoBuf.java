package loghub.decoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.protobuf.BinaryDecoder;
import lombok.Setter;

@BuilderClass(ProtoBuf.Builder.class)
public class ProtoBuf extends Decoder {

    private interface GetInputStream {
        InputStream get() throws IOException;
    }

    @Setter
    public static class Builder extends Decoder.Builder<ProtoBuf> {
        private String schemaUri;
        private String mappingClass;
        private ClassLoader loader;
        private String[] knowMessages = new String[]{};
        @Override
        public ProtoBuf build() {
            return new ProtoBuf(this);
        }
    }
    public static ProtoBuf.Builder getBuilder() {
        return new ProtoBuf.Builder();
    }

    private final BinaryDecoder decoder;
    private final String mappingClass;

    public ProtoBuf(Builder builder) {
        super(builder);
        mappingClass = builder.mappingClass;
        try {
             decoder = new BinaryDecoder(Helpers.fileUri(builder.schemaUri));
        } catch (Descriptors.DescriptorValidationException | IOException ex) {
            throw new IllegalStateException("Unusable binary schema :" + Helpers.resolveThrowableException(ex), ex);
        }
        decoder.addFastPath("com.google.protobuf.Timestamp", s -> {
            long seconds = s.readInt64();
            int nanos = s.readInt32();
            return Instant.ofEpochSecond(seconds, nanos);
        });
        for (String clazz: builder.knowMessages) {
            try {
                Class<?> loadedClass = builder.loader.loadClass(clazz);
                if (loadedClass.isAssignableFrom(GeneratedMessage.class)) {
                    Method builderMethod = loadedClass.getMethod("parseFrom", CodedInputStream.class);
                    decoder.addFastPath(clazz, s -> parseFrom(builderMethod, s));
                }
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Object parseFrom(Method builderMethod, CodedInputStream stream) {
        try {
            return builderMethod.invoke(null, stream);
        } catch (IllegalAccessException | InvocationTargetException ex) {
            throw new RuntimeException(ex);
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
            Map<String, Object> values = new HashMap<>();
            List<BinaryDecoder.UnknownField> unknownFields = new ArrayList<>();
            decoder.parseInput(CodedInputStream.newInstance(is), mappingClass, values, unknownFields);
            return values;
        } catch (IOException ex) {
            throw new DecodeException("Failed to decode Protobuf event: " + Helpers.resolveThrowableException(ex), ex);
        }
    }

}
