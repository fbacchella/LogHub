package loghub.decoders;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Properties.Builder.class)
public class Properties extends AbstractStringJackson {

    public static class Builder extends AbstractStringJackson.Builder<Properties> {
       @Setter
        private String keyValueSeparator= "=";
        @Setter
        private String pathSeparator= ".";
        @Setter
        private boolean parseSimpleIndexes = true;
        @Setter
        private int firstArrayOffset = 1;

        @Override
        public Properties build() {
            return new Properties(this);
        }
    };

    public static Builder getBuilder() {
        return new Builder();
    }

    private final ObjectReader reader;

    protected Properties(Builder builder) {
        super(builder);

        JavaPropsSchema schema = JavaPropsSchema.emptySchema();
        schema = schema.withPathSeparator(builder.pathSeparator);
        schema = schema.withKeyValueSeparator(builder.keyValueSeparator);
        schema = schema.withParseSimpleIndexes(builder.parseSimpleIndexes);
        schema = schema.withFirstArrayOffset(builder.firstArrayOffset);

        reader =  JacksonBuilder.get(JavaPropsMapper.class)
                .setMapperSupplier(JavaPropsMapper::new)
                .setSchema(schema)
                .getReader();
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
            throws DecodeException, IOException {
        return gen.deserialize(reader);
    }

}
