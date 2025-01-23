package loghub.decoders;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
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
import loghub.netflow.Netflow5Packet;
import loghub.netflow.Netflow9Packet;
import loghub.netflow.NetflowPacket;
import loghub.netflow.NetflowRegistry;
import loghub.netflow.Template;
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
    private EventsFactory factory;
    private final boolean snakeCase;
    private final boolean flowSignature;

    private Netflow(Builder builder) {
        super(builder);
        registry = new NetflowRegistry();
        snakeCase = builder.snakeCase;
        flowSignature = builder.flowSignature;
    }

    @Override
    public boolean configure(Properties properties, Receiver<?, ?> receiver) {
        factory = properties.eventsFactory;
        return super.configure(properties, receiver);
    }

    @Override
    public Object decodeObject(ConnectionContext<?> ctx, ByteBuf bbuf) throws DecodeException {
        InetAddress addr;
        if (ctx instanceof IpConnectionContext) {
            addr = ((IpConnectionContext) ctx).getRemoteAddress().getAddress();
            NetflowPacket packet = registry.parsePacket(addr, bbuf);
            Map<String, Object> decodedPacket = new HashMap<>();
            Instant eventTimestamp = packet.getExportTime();
            decodedPacket.put(convertName("sequenceNumber"), packet.getSequenceNumber());
            decodedPacket.put("version", packet.getVersion());
            decodedPacket.put("records", packet.getRecords());
            switch (packet.getVersion()) {
            case 5:
                Netflow5Packet packet5 = (Netflow5Packet) packet;
                decodedPacket.put(convertName("EngineType"), packet5.getEngineType());
                decodedPacket.put(convertName("SamplingInterval"), packet5.getSamplingInterval());
                decodedPacket.put(convertName("SamplingMode"), packet5.getSamplingMode());
                decodedPacket.put(convertName("SysUptime"), packet5.getSysUpTime());
                return splitV5Packet(ctx, eventTimestamp, decodedPacket);
            case 9:
                decodedPacket.put(convertName("SysUptime"), ((Netflow9Packet) packet).getSysUpTime());
                return splitTemplatePacket(ctx, eventTimestamp, decodedPacket);
            case 10:
                return splitTemplatePacket(ctx, eventTimestamp, decodedPacket);
            default:
                throw new UnsupportedOperationException();
            }
        } else {
            return null;
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

    private Object splitV5Packet(ConnectionContext<?> ctx, Instant eventTimestamp, Map<String, Object> decodedPacket) {
        List<Event> events = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) decodedPacket.remove("records");

        UUID msgUuid = UUID.randomUUID();

        records.forEach(i -> {
            Event newEvent = factory.newEvent(ctx);
            newEvent.setTimestamp(eventTimestamp);
            newEvent.putMeta("msgUUID", msgUuid);
            newEvent.putAll(convertMap(i));
            newEvent.putAll(decodedPacket);
            if (flowSignature) {
                makeFlowSignature(i).ifPresent(uuid -> newEvent.putMeta("flowSignature", uuid));
            }
            events.add(newEvent);
        });
        return events;
    }

    List<Event> splitTemplatePacket(ConnectionContext ctx, Instant eventTimestamp, Map<String, Object> decodedPacket) {
        List<Event> events = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) decodedPacket.remove("records");

        UUID msgUuid = UUID.randomUUID();

        records.forEach(i -> {
            Template.TemplateType recordType = (Template.TemplateType) i.remove(NetflowRegistry.TYPEKEY);
            Event newEvent = factory.newEvent(ctx);
            newEvent.setTimestamp(eventTimestamp);
            newEvent.putMeta("msgUUID", msgUuid);
            if (flowSignature) {
                makeFlowSignature(i).ifPresent(uuid -> newEvent.putMeta("flowSignature", uuid));
            }
            newEvent.putAll(decodedPacket);
            newEvent.putAll(convertMap(i));
            Throwable ex = (Throwable) i.remove(NetflowPacket.EXCEPTION_KEY);
            if (ex != null) {
                newEvent.pushException(ex);
            }
            if (recordType == Template.TemplateType.Options) {
                newEvent.putMeta("type", "option");
            } else if (recordType == Template.TemplateType.Records) {
                newEvent.putMeta("type", "flow");
            } else {
                newEvent.putMeta("type", "unknown");
            }
            events.add(newEvent);
        });
        return events;
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
