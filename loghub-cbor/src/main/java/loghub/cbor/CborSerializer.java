package loghub.cbor;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

public class CborSerializer<C, T> extends JsonSerializer<C> {

    private final CborTagHandler<T> tag;
    private final Class<C> handledClass;

    public CborSerializer(CborTagHandler<T> tag, Class<C> handledClass) {
        this.tag = tag;
        this.handledClass = handledClass;
    }

    @Override
    public Class<C> handledType() {
        return handledClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serialize(C value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        ((CBORGenerator)gen).writeTag(tag.getTag());
        tag.doWrite((T) value, (CBORGenerator) gen);
    }

}
