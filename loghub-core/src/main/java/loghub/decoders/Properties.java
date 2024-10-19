package loghub.decoders;

import com.fasterxml.jackson.dataformat.javaprop.JavaPropsMapper;
import com.fasterxml.jackson.dataformat.javaprop.JavaPropsSchema;

import loghub.BuilderClass;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Properties.Builder.class)
public class Properties extends AbstractStringJackson<Properties.Builder, JavaPropsMapper> {

    @Setter
    public static class Builder extends AbstractStringJackson.Builder<Properties> {
       private String keyValueSeparator= "=";
        private String pathSeparator= ".";
        private boolean parseSimpleIndexes = true;
        private int firstArrayOffset = 1;

        @Override
        public Properties build() {
            return new Properties(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private Properties(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<JavaPropsMapper> getReaderBuilder(Builder builder) {
        JavaPropsSchema schema = JavaPropsSchema.emptySchema();
        schema = schema.withPathSeparator(builder.pathSeparator);
        schema = schema.withKeyValueSeparator(builder.keyValueSeparator);
        schema = schema.withParseSimpleIndexes(builder.parseSimpleIndexes);
        schema = schema.withFirstArrayOffset(builder.firstArrayOffset);

        return JacksonBuilder.get(JavaPropsMapper.class)
                             .setSchema(schema);
    }

}
