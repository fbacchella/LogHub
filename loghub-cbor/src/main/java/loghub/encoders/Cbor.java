package loghub.encoders;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.cbor.CborTagHandlerService;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Cbor.Builder.class)
@CanBatch
public class Cbor extends AbstractJacksonEncoder<Cbor.Builder, CBORMapper> {

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

}
