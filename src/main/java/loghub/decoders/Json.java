package loghub.decoders;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;

import loghub.BuilderClass;
import loghub.ConnectionContext;

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

    private static final ObjectReader reader;
    static {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        mapper.setDefaultTyping(StdTypeResolverBuilder.noTypeInfoBuilder());
        reader = mapper.readerFor(OBJECTREF);
    }

    protected Json(Builder builder) {
        super(builder);
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
                    throws DecodeException, IOException {
        return gen.deserialize(reader);
    }

}
