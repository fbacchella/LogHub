package loghub.decoders;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

import io.netty.buffer.ByteBuf;
import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.netflow.IpfixPacket;
import loghub.netflow.Netflow5Packet;
import loghub.netflow.Netflow9Packet;
import loghub.netflow.NetflowPacket;
import loghub.netflow.NetflowRegistry;
import loghub.netflow.Template;
import loghub.netflow.TemplateBasedPacket;
import loghub.receivers.Receiver;
import lombok.Setter;

@BuilderClass(Netflow.Builder.class)
public class Netflow extends Decoder {

    private static final ThreadLocal<MessageDigest> MD5;
    static {
        MD5 = ThreadLocal.withInitial( () -> {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        });
    }
    private static final Base64.Encoder b64encoder = Base64.getEncoder().withoutPadding();

    @Setter
    public static class Builder extends Decoder.Builder<Netflow> {
        private boolean snakeCase = false;
        private boolean flowSignature = false;
        private EventsFactory factory;
        @Override
        public Netflow build() {
            return new Netflow(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private static final Pattern SNAKE_CASE1 = Pattern.compile("([a-z0-9])([A-Z]+)");      // Handle aA and like
    private static final Pattern SNAKE_CASE2 = Pattern.compile("(IP)(v[?46])");            // Handle IPv that is broken in the latter rule
    private static final Pattern SNAKE_CASE3 = Pattern.compile("([A-Z]+)([A-Z][a-z])");    // Handle HTTPRequest and like

    private final NetflowRegistry registry;
    private final EventsFactory factory;
    private final boolean snakeCase;
    private final boolean flowSignature;

    private Netflow(Builder builder) {
        super(builder);
        registry = new NetflowRegistry();
        snakeCase = builder.snakeCase;
        flowSignature = builder.flowSignature;
        factory = builder.factory;
    }

    @Override
    public boolean configure(Properties properties, Receiver<?, ?> receiver) {
        return super.configure(properties, receiver);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        InetAddress addr;
        if (ctx instanceof IpConnectionContext) {
            addr = ((IpConnectionContext) ctx).getRemoteAddress().getAddress();
            NetflowPacket packet;
            try {
                packet = registry.parsePacket(addr, bbuf);
            } catch (IOException e) {
                throw new DecodeException("Failed decoding packet", e);
            }
            UUID msgUuid = UUID.randomUUID();
            switch (packet.getVersion()) {
            case 5:
                return splitV5Packet(ctx, msgUuid, (Netflow5Packet) packet);
            case 9:
                return splitV9Packet(ctx, msgUuid, (Netflow9Packet) packet);
            case 10:
                return splitIpfixPacket(ctx, msgUuid, (IpfixPacket) packet);
            default:
                throw new UnsupportedOperationException("Unhandled packet version " + packet.getVersion());
            }
        } else {
            return null;
        }
    }

    private List<Event> splitV5Packet(ConnectionContext<?> ctx, UUID msgUuid, Netflow5Packet packet) {
        List<Event> events = new ArrayList<>();
        String engineTypeName = convertName("engineType");
        String engineIdName = convertName("engineId");
        String samplingIntervalName = convertName("samplingInterval");
        String samplingModeName = convertName("samplingMode");
        String sysUptimeName = convertName("sysUpTime");
        packet.getRecords().forEach(i -> {
            Event newEvent = newEvent(ctx, packet, msgUuid, i);
            newEvent.put(engineTypeName, packet.getEngineType());
            newEvent.put(engineIdName, packet.getId());
            newEvent.put(samplingIntervalName, packet.getSamplingInterval());
            newEvent.put(samplingModeName, packet.getSamplingMode());
            newEvent.put(sysUptimeName, packet.getSysUpTime());
            events.add(newEvent);
        });
        return events;
    }

    List<Event> splitV9Packet(ConnectionContext<?> ctx, UUID msgUuid, Netflow9Packet packet) {
        List<Event> events = splitTemplatePacket(ctx, msgUuid, packet);
        String sysUptimeName = convertName("sysUpTime");
        String sourceIdName = convertName("sourceId");
        events.forEach(ev -> {
            ev.put(sysUptimeName, packet.getSysUpTime());
            ev.put(sourceIdName, packet.getId());
        });
        return events;
    }

    List<Event> splitIpfixPacket(ConnectionContext<?> ctx, UUID msgUuid, IpfixPacket packet) {
        List<Event> events = splitTemplatePacket(ctx, msgUuid, packet);
        String observationDomainIdName = convertName("observationDomainId");
        events.forEach(ev -> ev.put(observationDomainIdName, packet.getId()));
        return events;
    }

    List<Event> splitTemplatePacket(ConnectionContext<?> ctx, UUID msgUuid, TemplateBasedPacket packet) {
        List<Event> events = new ArrayList<>();

        packet.getRecords().forEach(i -> {
            Event newEvent = newEvent(ctx, packet, msgUuid, i);
            events.add(newEvent);
        });
        return events;
    }

    private Event newEvent(ConnectionContext<?> ctx, NetflowPacket packet, UUID msgUuid, Map<String, Object> data) {
        Event newEvent = factory.newEvent(ctx);
        newEvent.setTimestamp(packet.getExportTime());
        newEvent.putMeta("msgUUID", msgUuid);
        Throwable ex = (Throwable) data.remove(NetflowPacket.EXCEPTION_KEY);
        if (ex != null) {
            newEvent.pushException(ex);
        }
        Template.TemplateType recordType = (Template.TemplateType) data.remove(NetflowRegistry.TYPEKEY);
        if (recordType == Template.TemplateType.OPTIONS) {
            newEvent.putMeta("type", "option");
        } else if (recordType == Template.TemplateType.RECORDS) {
            newEvent.putMeta("type", "flow");
        } else {
            newEvent.putMeta("type", "unknown");
        }
        buildMeta(newEvent, data);
        if (flowSignature) {
            makeFlowSignature(data).ifPresent(uuid -> newEvent.putMeta("flowSignature", uuid));
        }
        newEvent.putAll(convertMap(data));
        newEvent.put(convertName("sequenceNumber"), packet.getSequenceNumber());
        newEvent.put("version", packet.getVersion());
        return newEvent;
    }

    private void buildMeta(Event event, Map<String, Object> data) {
        Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            if (entry.getKey().startsWith("__")) {
                event.putMeta(convertName(entry.getKey().substring(2)), entry.getValue());
                iterator.remove();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertMap(Map<String, Object> map) {
        if (snakeCase) {
            Map<String, Object> newMap = new HashMap<>(map.size());
            for (Map.Entry<String, Object> e: map.entrySet()) {
                Object value = e.getValue();
                if (value instanceof Map) {
                    value = convertMap((Map<String, Object>) value);
                }
                newMap.put(convertName(e.getKey()), value);
            }
            return newMap;
        } else {
            return map;
        }
     }

    private Optional<Object> makeFlowSignature(Map<String, Object> flow) {
        List<String> idElements = new ArrayList<>();
        for (String i: List.of("srcaddr", "dstaddr", "srcport", "dstport", "prot",
                               "sourceIPv4Address", "sourceTransportPort",
                               "destinationIPv4Address", "destinationTransportPort",
                               "protocolIdentifier")) {
            if (flow.containsKey(i)) {
                String prefix = "other:";
                switch (i) {
                case "srcaddr":
                case "dstaddr":
                case "sourceIPv4Address":
                case "destinationIPv4Address":
                    prefix = "ip:";
                    break;
                case "srcport":
                case "dstport":
                case "sourceTransportPort":
                case "destinationTransportPort":
                    prefix = "port:";
                    break;
                case "prot":
                case "protocolIdentifier":
                    prefix = "proto:";
                    break;
                }
                idElements.add(prefix + flow.get(i));
            }
        }
        if (! idElements.isEmpty()) {
            Collections.sort(idElements);
            MessageDigest localMD5 = MD5.get();
            localMD5.reset();
            idElements.forEach(s -> localMD5.update(s.getBytes(StandardCharsets.UTF_8)));
            String encoded = b64encoder.encodeToString(localMD5.digest());
            return Optional.of(encoded);
        } else {
            return Optional.empty();
        }
    }

    public String convertName(String name) {
        if (snakeCase) {
            name = SNAKE_CASE1.matcher(name).replaceAll("$1_$2");
            name = SNAKE_CASE2.matcher(name).replaceAll("ip$2");
            name = SNAKE_CASE3.matcher(name).replaceAll("$1_$2");
            name = name.toLowerCase();
        }
        return name;
    }

}
