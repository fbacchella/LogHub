package loghub.decoders;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;

import io.kaitai.struct.ByteBufferKaitaiStream;
import io.kaitai.struct.KaitaiStream;
import io.kaitai.struct.KaitaiStruct;
import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import lombok.Setter;

public class KaitaiDecoder extends Decoder {

    private static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {
    };

    private static final MethodHandles.Lookup LOOKUP = MethodHandles.lookup();

    @Setter
    public static class Builder extends Decoder.Builder<KaitaiDecoder> {
        private String kaitaiStruct;
        @Override
        public KaitaiDecoder build() {
            return new KaitaiDecoder(this);
        }
    }

    private final ObjectMapper mapper;
    private final MethodHandle kaitaiStructConstructor;

    private KaitaiDecoder(Builder builder) {
        super(builder);
        mapper = new ObjectMapper();
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.FIELD, Visibility.NONE);
        mapper.setVisibility(PropertyAccessor.GETTER, Visibility.PUBLIC_ONLY);
        mapper.setVisibility(PropertyAccessor.IS_GETTER, Visibility.ANY);
        mapper.setAnnotationIntrospector(new JacksonAnnotationIntrospector() {
            @Override
            public boolean hasIgnoreMarker(AnnotatedMember m) {
                String name = m.getName();
                if (name.startsWith("_") || super.hasIgnoreMarker(m)) {
                    return true;
                }
                // Only filter on names that look like property names (not the internal ones)
                // Actually hasIgnoreMarker is called on fields and methods.
                return false; 
            }
            @Override
            public PropertyName findNameForSerialization(Annotated a) {
                if (a instanceof AnnotatedMethod) {
                    AnnotatedMethod am = (AnnotatedMethod) a;
                    String name = am.getName();
                    if (name.startsWith("_")) {
                        return null;
                    } else if ((name.startsWith("get") && name.length() > 3 && Character.isUpperCase(name.charAt(3))) ||
                               (name.startsWith("is") && name.length() > 2 && Character.isUpperCase(name.charAt(2)))) {
                        return null;
                    } else if (am.getParameterCount() == 0 && am.getRawReturnType() != void.class) {
                        if (!name.equals("getClass") &&
                            !name.equals("hashCode") &&
                            !name.equals("toString") &&
                            !name.equals("clone")) {
                            return PropertyName.construct(name);
                        }
                    }
                }
                return super.findNameForSerialization(a);
            }
        });
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
    protected Object decodeObject(ConnectionContext<?> ctx, byte[] msg) throws DecodeException {
        KaitaiStream stream = new ByteBufferKaitaiStream(msg);
        return decodeKaitai(stream);
    }

    private Object decodeKaitai(KaitaiStream stream) throws DecodeException {
        try {
            KaitaiStruct struct = (KaitaiStruct) kaitaiStructConstructor.invoke(stream);
            return mapper.convertValue(struct, MAP_TYPE_REFERENCE);
        } catch (Throwable e) {
            throw new DecodeException("", e);
        }
    }

}
