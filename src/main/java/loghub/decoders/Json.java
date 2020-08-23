package loghub.decoders;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectReader;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.jackson.JacksonBuilder;

@BuilderClass(Json.Builder.class)
public class Json extends AbstractStringJackson {

    public static class Builder extends AbstractStringJackson.Builder<Json> {
        @Override
        public Json build() {
            return new Json(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private final ObjectReader reader;

    protected Json(Builder builder) {
        super(builder);
        reader = JacksonBuilder.get()
                .setFactory(new JsonFactory())
                .getReader();
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
                    throws DecodeException, IOException {
        return gen.deserialize(reader);
    }

}
