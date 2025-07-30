package loghub.encoders;

import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.cbor.CborTagHandlerService;
import loghub.cbor.LogHubEventTagHandler;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Cbor.Builder.class)
@CanBatch
public class Cbor extends AbstractJacksonEncoder<Cbor.Builder, CBORMapper> {

    @Setter
    public static class Builder extends AbstractJacksonEncoder.Builder<Cbor> {
        private boolean forwardEvent = false;
        private EventsFactory factory;
        private ClassLoader clLoader;
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
        CborTagHandlerService service = new CborTagHandlerService(builder.clLoader);
        if (builder.forwardEvent) {
            service.getByTag(LogHubEventTagHandler.EVENT_TAG)
                   .map(LogHubEventTagHandler.class::cast)
                   .ifPresent(th -> setParser(th, builder.factory));
        }
        service.makeSerializers().forEach(jbuilder::addSerializer);
        return jbuilder;
    }

    private void setParser(LogHubEventTagHandler handler, EventsFactory factory) {
        handler.setCustomParser(LogHubEventTagHandler.eventParser(factory));
    }

}
