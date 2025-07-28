package loghub.decoders;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

import org.antlr.v4.runtime.CharStreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Helpers;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.jackson.JacksonBuilder;
import loghub.receivers.Receiver;
import loghub.sflow.SFlowDatagram;
import loghub.sflow.SflowParser;
import loghub.sflow.structs.Struct;
import loghub.xdr.TypeSpecifier;
import loghub.xdr.XdrWalker;
import lombok.Setter;

@BuilderClass(Sflow.Builder.class)
public class Sflow extends Decoder {

    private static final List<String> EMBEDED_STRUCS = List.of(
            "sflow_datagram", "sflow",
            "bv-sflow", "host", "sflow_80211", "sflow_application", "sflow_broadcom_tables", "sflow_drops", "sflow_host_ip", "sflow_http", "sflow_jvm",
            "sflow_lag", "sflow_memcached", "sflow_nvml", "sflow_openflow", "sflow_optics", "sflow_transit", "sflow_tunnels"
    );

    @Setter
    public static class Builder extends Decoder.Builder<Sflow> {
        String[] xdrPaths = new String[]{};
        ClassLoader classLoader = getClass().getClassLoader();
        EventsFactory factory;
        @Override
        public Sflow build() {
            return new Sflow(this);
        }
    }

    public static Sflow.Builder getBuilder() {
        return new Sflow.Builder();
    }

    private final ObjectMapper objectMapper = JacksonBuilder.get(JsonMapper.class).getMapper();
    private final SflowParser sflowRegistry = new SflowParser();
    private final EventsFactory factory;

    protected Sflow(Builder builder) {
        super(builder);
        Map<String, TypeSpecifier<?>> knownTypes = new HashMap<>();
        XdrWalker xdrWalker = new XdrWalker();
        Consumer<URL> readXdr = s -> {
            logger.debug("Parsing xdr file {}", s);
            try (InputStream is = s.openStream()) {
                knownTypes.putAll(xdrWalker.startWalk(CharStreams.fromStream(is), knownTypes));
            } catch (IOException ex) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? ex : null).log("Unusable xdr \"{}",  () -> logger.isDebugEnabled() ? "" : ": " + Helpers.resolveThrowableException(ex));
            }
        };
        EMBEDED_STRUCS
            .stream()
            .map(s  -> String.format("xdr/%s.xdr", s))
            .map(s -> builder.classLoader.getResource(s))
            .forEach(readXdr);
        Arrays.stream(builder.xdrPaths).map(s -> {
            try {
                return Helpers.fileUri(s).toURL();
            } catch (MalformedURLException ex) {
                logger.atError().withThrowable(logger.isDebugEnabled() ? ex : null).log("Unusable xdr \"{}\"",  s);
                return null;
            }
        }).filter(Objects::nonNull).forEach(readXdr);
        sflowRegistry.addTypes(knownTypes);
        factory = builder.factory;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        List<Event> events = new ArrayList<>();
        UUID msgUuid = UUID.randomUUID();

        try {
            SFlowDatagram sFlowHeader = sflowRegistry.decodePacket(bbuf);
            Map<String, Object> observer = objectMapper.convertValue(sFlowHeader, Map.class);
            for (Struct s: sFlowHeader.getSamples()) {
                Map<String, Object> data = objectMapper.convertValue(s, Map.class);
                Event ev = factory.newEvent(ctx);
                ev.putMeta("msgUUID", msgUuid);
                ev.put(s.getName(), data);
                ev.putAtPath(VariablePath.of("observer"), observer);
                events.add(ev);
            }
        } catch (IOException | RuntimeException e) {
            throw new DecodeException("Failed decoding sFlow packet from " + ctx.getRemoteAddress(), e);
        }
        return events;
    }

}
