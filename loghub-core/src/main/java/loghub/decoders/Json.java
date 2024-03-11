package loghub.decoders;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.BuilderClass;
import loghub.jackson.JacksonBuilder;

@BuilderClass(Json.Builder.class)
public class Json extends AbstractStringJackson<Json.Builder, JsonMapper> {

    public static class Builder extends AbstractStringJackson.Builder<Json> {
        @Override
        public Json build() {
            this.charset = StandardCharsets.UTF_8.name();
            return new Json(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    protected Json(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<JsonMapper> getReaderBuilder(Builder builder) {
        return JacksonBuilder.get(JsonMapper.class);
    }

}
