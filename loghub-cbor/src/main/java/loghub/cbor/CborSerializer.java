package loghub.cbor;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;

public class CborSerializer<T> extends JsonSerializer<T> {

    private final CborTagHandler<T> tag;
    private final Class<T> handledClass;

    @SuppressWarnings("unchecked")
    public CborSerializer(Class<T> handledClass) {
        this.tag = (CborTagHandler<T>) CborTagHandlerService.getByType(handledClass)
                                                            .orElseThrow();
        this.handledClass = handledClass;
    }

    @Override
    public Class<T> handledType() {
        return handledClass;
    }

    @Override
    public void serialize(T value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        ((CBORGenerator)gen).writeTag(tag.getTag());
        tag.write(value, (CBORGenerator) gen);
    }

}
