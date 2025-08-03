package loghub.encoders;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.cbor.CborTagHandlerService;
import loghub.jackson.JacksonBuilder;
import loghub.types.MimeType;
import lombok.Setter;

@BuilderClass(Cbor.Builder.class)
@CanBatch
public class Cbor extends AbstractJacksonEncoder<Cbor.Builder, CBORMapper> {

    public static final MimeType MIME_TYPE = MimeType.of("application/cbor");

    @Setter
    public static class Builder extends AbstractJacksonEncoder.Builder<Cbor> {
        private ClassLoader classLoader;
        @Override
        public Cbor build() {
            return new Cbor(this);
        }
    }
    public static Cbor.Builder getBuilder() {
        return new Cbor.Builder();
    }

    private Cbor(Cbor.Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<CBORMapper> getWriterBuilder(Cbor.Builder builder) {
        JacksonBuilder<CBORMapper> jbuilder = JacksonBuilder.get(CBORMapper.class);
        CborTagHandlerService service = new CborTagHandlerService(builder.classLoader);
        service.makeSerializers().forEach(jbuilder::addSerializer);
        return jbuilder;
    }

    @Override
    public MimeType getMimeType() {
        return MIME_TYPE;
    }

}
