package loghub.decoders;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.cbor.CborParser.CborParserFactory;
import loghub.cbor.CborTagHandlerService;
import loghub.configuration.Properties;
import loghub.events.EventsFactory;
import loghub.receivers.Receiver;
import lombok.Setter;

@BuilderClass(Cbor.Builder.class)
public class Cbor extends Decoder {

    @Setter
    public static class Builder extends Decoder.Builder<Cbor> {
        private ClassLoader classLoader = Cbor.class.getClassLoader();
        private EventsFactory eventsFactory;
        @Override
        public Cbor build() {
            return new Cbor(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final CborParserFactory factory;
    private Cbor(Builder builder) {
        super(builder);
        factory = new CborParserFactory(new CborTagHandlerService(builder.classLoader));
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> connectionContext, byte[] msg, int offset, int length)
            throws DecodeException {
        try {
            return factory.getParser(msg, offset, length).run();
        } catch (IOException e) {
            throw new DecodeException("Unable to read CBOR buffer", e);
        }
    }

    @Override
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        try {
            return factory.getParser(new ByteBufInputStream(bbuf)).run();
        } catch (IOException e) {
            throw new DecodeException("Unable to read CBOR buffer", e);
        }
    }

}
