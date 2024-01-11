package loghub.encoders;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(ToJson.Builder.class)
@CanBatch
public class ToJson extends AbstractJacksonEncoder<ToJson.Builder, JsonMapper> {
    
    public static class Builder extends AbstractJacksonEncoder.Builder<ToJson> {
        @Setter
        private boolean pretty = false;
        @Setter
        private boolean dateAsText = false;
        @Override
        public ToJson build() {
            return new ToJson(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private ToJson(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<JsonMapper> getWriterBuilder(Builder builder) {
        JacksonBuilder<JsonMapper> jbuilder = JacksonBuilder.get(JsonMapper.class)
                .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, ! (builder.pretty || builder.dateAsText)))
                ;
        if (builder.pretty) {
            jbuilder.feature(SerializationFeature.INDENT_OUTPUT);
        }
        return jbuilder;
    }

}
