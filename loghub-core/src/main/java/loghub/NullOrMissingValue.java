package loghub;

import java.io.IOException;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public abstract class NullOrMissingValue {

    public static class JacksonModule extends SimpleModule {
        @Override
        public void setupModule(SetupContext context) {
            super.setupModule(context);
            SimpleSerializers sers = new SimpleSerializers();
            sers.addSerializer(new MissingSerializer());
            sers.addSerializer(new NullSerializer());
            context.addSerializers(sers);
        }
    }

    private abstract static class NullOrMissingValueSerializer<T extends NullOrMissingValue> extends StdSerializer<T> {
        protected NullOrMissingValueSerializer(Class<T> clazz) {
            super(clazz);
        }

        @Override
        public void serialize(NullOrMissingValue value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeNull();
        }
    }

    private static class MissingSerializer extends NullOrMissingValueSerializer<Missing> {
        public MissingSerializer() {
            super(Missing.class);
        }
    }

    private static class NullSerializer extends NullOrMissingValueSerializer<Null> {
        public NullSerializer() {
            super(Null.class);
        }
    }

    private static class Missing extends NullOrMissingValue {
        @Override
        public String toString() {
            return "NoValue";
        }
    }

    private static class Null extends NullOrMissingValue {
        @Override
        public String toString() {
            return "NullValue";
        }
    }

    public static final NullOrMissingValue MISSING = new Missing();

    public static final NullOrMissingValue NULL = new Null();

    private NullOrMissingValue() {
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object)null);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == null || obj instanceof NullOrMissingValue;
    }

    public boolean compareTo(Object obj) {
        return obj == null || obj instanceof NullOrMissingValue;
    }

}
