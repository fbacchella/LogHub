package loghub.decoders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.receivers.Receiver;
import loghub.sflow.SFlowDatagram;
import loghub.sflow.SflowParser;
import loghub.sflow.structs.Struct;
import lombok.Setter;

@BuilderClass(Sflow.Builder.class)
public class Sflow extends Decoder {

    @Setter
    public static class Builder extends Decoder.Builder<Sflow> {
        @Override
        public Sflow build() {
            return new Sflow(this);
        }
    }

    public static Sflow.Builder getBuilder() {
        return new Sflow.Builder();
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SflowParser sflowRegistry = new SflowParser();
    private EventsFactory factory;


    protected Sflow(Builder builder) {
        super(builder);
    }

    @Override
    public boolean configure(Properties properties, Receiver<?, ?> receiver) {
        factory = properties.eventsFactory;
        return super.configure(properties, receiver);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        List<Event> events = new ArrayList<>();

        try {
            SFlowDatagram sFlowHeader = sflowRegistry.decodePacket(bbuf);
            Map<String, Object> observer = objectMapper.convertValue(sFlowHeader, Map.class);
            for (Struct s: sFlowHeader.getSamples()) {
                Map<String, Object> data = objectMapper.convertValue(s, Map.class);
                Event ev = factory.newEvent(ctx);
                ev.putAll(data);
                ev.putAtPath(VariablePath.of("observer"), observer);
                events.add(ev);
            }
        } catch (IOException e) {
            throw new DecodeException(Helpers.resolveThrowableException(e), e);
        }
        return events;
    }

}
