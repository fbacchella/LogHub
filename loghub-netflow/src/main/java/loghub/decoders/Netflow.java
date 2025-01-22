package loghub.decoders;

import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

@BuilderClass(Netflow.Builder.class)
public class Netflow extends Decoder {

    public static class Builder extends Decoder.Builder<Netflow> {
        @Override
        public Netflow build() {
            return new Netflow(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    private final NetflowRegistry registry;
    private EventsFactory factory;

    private Netflow(loghub.decoders.Decoder.Builder<? extends Decoder> builder) {
        super(builder);
        registry = new NetflowRegistry();
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
            decodedPacket.put("sequenceNumber", packet.getSequenceNumber());
            decodedPacket.put("version", packet.getVersion());
            decodedPacket.put("records", packet.getRecords());
            switch (packet.getVersion()) {
            case 5:
                Netflow5Packet packet5 = (Netflow5Packet) packet;
                decodedPacket.put("engine_type", packet5.getEngineType());
                decodedPacket.put("sampling_interval", packet5.getSamplingInterval());
                decodedPacket.put("sampling_mode", packet5.getSamplingMode());
                decodedPacket.put("SysUptime", packet5.getSysUpTime());
                return splitV5Packet(ctx, eventTimestamp, decodedPacket);
            case 9:
                decodedPacket.put("SysUptime", ((Netflow9Packet) packet).getSysUpTime());
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

    private Object splitV5Packet(ConnectionContext<?> ctx, Instant eventTimestamp, Map<String, Object> decodedPacket) {
        List<Event> events = new ArrayList<>();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) decodedPacket.remove("records");

        UUID msgUuid = UUID.randomUUID();

        records.forEach(i -> {
            Event newEvent = factory.newEvent(ctx);
            newEvent.setTimestamp(eventTimestamp);
            newEvent.putMeta("msgUUID", msgUuid);
            newEvent.putAll(i);
            newEvent.putAll(decodedPacket);
            events.add(newEvent);
        });
        return events;
    }

    List<Event> splitTemplatePacket(ConnectionContext ctx, Instant eventTimestamp, Map<String, Object> decodedPacket) {
        List<Event> events = new ArrayList<>();

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) decodedPacket.remove("records");

        UUID msgUuid = UUID.randomUUID();

        Map<String, Object> options = new HashMap<>();
        records.forEach(i -> {
            Template.TemplateType recordType = (Template.TemplateType) i.remove(NetflowRegistry.TYPEKEY);
            if (recordType == Template.TemplateType.Options) {
                options.clear();
                options.putAll(i);
            } else if (recordType == Template.TemplateType.Records) {
                Event newEvent = factory.newEvent(ctx);
                newEvent.setTimestamp(eventTimestamp);
                newEvent.putMeta("msgUUID", msgUuid);
                newEvent.putAll(i);
                newEvent.putAll(decodedPacket);
                if (! options.isEmpty()) {
                    newEvent.put("options", options);
                }
                Throwable ex = (Throwable) i.remove(NetflowPacket.EXCEPTION_KEY);
                if (ex != null) {
                    newEvent.pushException(ex);
                }
                events.add(newEvent);
            }
        });
        return events;
    }

}
