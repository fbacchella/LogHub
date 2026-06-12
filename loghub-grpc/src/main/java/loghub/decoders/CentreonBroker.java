package loghub.decoders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;

import loghub.BuilderClass;
import loghub.NullOrMissingValue;
import loghub.decoders.CentreonBroker.Builder;
import loghub.grpc.BinaryCodec;
import loghub.grpc.BinaryCodec.UnknownField;
import loghub.grpc.GrpcStreamHandler;
import loghub.grpc.GrpcStreamHandler.Factory;
import loghub.receivers.GrpcReceiver;
import lombok.Setter;

@BuilderClass(Builder.class)
public class CentreonBroker extends ProtoBuf implements CodecProvider {

    @Setter
    public static class Builder extends ProtoBuf.Builder {
        private String name = "BBDO-Client-Output-LogHub";
        @Override
        public CentreonBroker build() {
            return new CentreonBroker(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final AtomicInteger messageCountAtomic = new AtomicInteger();
    private final String name;

    public CentreonBroker(Builder builder) {
        super(builder);
        this.name = builder.name;
    }

    @Override
    protected BinaryCodec getDecoder(ProtoBuf.Builder builder) throws DescriptorValidationException, IOException {
        BinaryCodec bc = new BinaryCodec("Centreon", CentreonAgent.class.getClassLoader().getResourceAsStream("centreon.binpb"));
        bc.addFastPath("com.centreon.broker.stream.CentreonEvent.buffer", this::resolveBuffer);
        // Service
        bc.addFastPath("com.centreon.broker.Service.last_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_hard_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_notification", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_time_ok", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_time_warning", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_time_critical", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_time_unknown", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.last_update", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.next_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Service.next_notification", this::resolveTimestamp);
        // AdaptiveServiceStatus
        bc.addFastPath("com.centreon.broker.AdaptiveServiceStatus.next_check", this::resolveTimestamp);
        // ServiceCheck
        bc.addFastPath("com.centreon.broker.ServiceCheck.next_check", this::resolveTimestamp);
        // ServiceStatus
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_hard_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_time_ok", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_time_warning", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_time_critical", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_time_unknown", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.next_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.last_notification", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.ServiceStatus.next_notification", this::resolveTimestamp);
        // Host
        bc.addFastPath("com.centreon.broker.Host.last_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_hard_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_notification", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_time_down", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_time_unreachable", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_time_up", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.last_update", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.next_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Host.next_host_notification", this::resolveTimestamp);
        // AdaptiveHostStatus
        bc.addFastPath("com.centreon.broker.AdaptiveHostStatus.next_check", this::resolveTimestamp);
        // HostCheck
        bc.addFastPath("com.centreon.broker.HostCheck.next_check", this::resolveTimestamp);
        // HostStatus
        bc.addFastPath("com.centreon.broker.HostStatus.last_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_hard_state_change", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_time_up", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_time_down", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_time_unreachable", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.next_check", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.last_notification", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.HostStatus.next_host_notification", this::resolveTimestamp);
        // LogEntry
        bc.addFastPath("com.centreon.broker.LogEntry.ctime", this::resolveTimestamp);
        // Downtime
        bc.addFastPath("com.centreon.broker.Downtime.entry_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Downtime.actual_start_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Downtime.actual_end_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Downtime.start_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Downtime.deletion_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Downtime.end_time", this::resolveTimestamp);
        // Acknowledgement
        bc.addFastPath("com.centreon.broker.Acknowledgement.entry_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Acknowledgement.deletion_time", this::resolveTimestamp);
        // Comment
        bc.addFastPath("com.centreon.broker.Comment.deletion_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Comment.entry_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Comment.expire_time", this::resolveTimestamp);
        // CustomVariable
        bc.addFastPath("com.centreon.broker.CustomVariable.update_time", this::resolveTimestamp);
        // CustomVariableStatus
        bc.addFastPath("com.centreon.broker.CustomVariableStatus.update_time", this::resolveTimestamp);
        // Instance
        bc.addFastPath("com.centreon.broker.Instance.end_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Instance.start_time", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.Instance.cma_cert_peremption", this::resolveTimestamp);
        // InstanceStatus
        bc.addFastPath("com.centreon.broker.InstanceStatus.last_alive", this::resolveTimestamp);
        bc.addFastPath("com.centreon.broker.InstanceStatus.last_command_check", this::resolveTimestamp);

        bc.addFastPath("com.centreon.broker.Metric.time", this::resolveTimestamp);

        return bc;
    }

    private Object resolveTimestamp(CodedInputStream codedInputStream, FieldDescriptor fieldDescriptor, List<UnknownField> unknownFields)
            throws IOException {
        long ts = codedInputStream.readUInt64();
        if (ts != 0) {
            return Instant.ofEpochSecond(ts);
        } else {
            return NullOrMissingValue.MISSING;
        }
    }

    private Object resolveBuffer(CodedInputStream codedInputStream, FieldDescriptor fieldDescriptor, List<UnknownField> unknownFields)
            throws IOException {
        ByteBuffer bb = codedInputStream.readByteBuffer();
        return BbdoPacket.of(decoder, bb);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void registerFastPath(Factory factory, GrpcReceiver r) {
        factory.register("com.centreon.broker.stream.centreon_bbdo.exchange",
                (c, m) -> exchange(r, c, (Map<String, Object>) m)
        );
    }

    private Map<String, Object> exchange(GrpcReceiver r, GrpcStreamHandler<?, ?> handler, Map<String, Object> message) {
        logger.trace("Received exchange message {}", message);
        if (message.containsKey("buffer")) {
            BbdoPacket inPacket = (BbdoPacket) message.get("buffer");
            BbdoPacket outPacket = switch (inPacket.event) {
                case BbdoEvent.BBDO_WELCOME -> {
                    Map<String, Object> welcome = new HashMap<>(inPacket.payload);
                    welcome.put("peer_type", "BROKER");
                    welcome.remove("extensions");
                    welcome.put("broker_name", name);
                    yield new BbdoPacket(BbdoEvent.BBDO_WELCOME, inPacket.destinationId, inPacket.sourceId, welcome);
                }
                case BbdoEvent.BBDO_PB_STOP -> {
                    int toAck = messageCountAtomic.getAndSet(0);
                    yield new BbdoPacket(BbdoEvent.BBDO_PB_ACK, inPacket.destinationId, inPacket.sourceId, Map.of("acknowledged_events", toAck));
                }
                case BbdoEvent.STORAGE_PB_METRIC, STORAGE_PB_STATUS -> null;
                case Object o when inPacket.event.namespace == 3 -> null;
                default -> {
                    Map<String, Object> buffer = Map.of(
                            "event", inPacket.event,
                            "destination_id", inPacket.destinationId,
                            "source_id", inPacket.sourceId,
                            "data", inPacket.payload
                    );
                    message.put("buffer", buffer);
                    r.publish(handler, Stream.of(message));
                    yield null;
                }
            };
            if (messageCountAtomic.incrementAndGet() > 1000 && outPacket == null) {
                int toAck = messageCountAtomic.getAndSet(0);
                outPacket = new BbdoPacket(BbdoEvent.BBDO_PB_ACK, inPacket.destinationId, inPacket.sourceId, Map.of("acknowledged_events", toAck));
            }
            if (outPacket != null) {
                Object sourceId = message.get("source_id");
                Object destinationId = message.get("destination_id");
                return Map.of("buffer", ByteString.copyFrom(outPacket.serialize(decoder)), "source_id", destinationId, "destination_id", sourceId);
            }
        } else {
            r.publish(handler, Stream.of(message));
        }
        return Map.of();
    }

    public enum BbdoEvent {
        // NEB category (namespace=1), pb_ messages only (de_pb_* from events.hh)
        NEB_PB_SERVICE(1, 27, "com.centreon.broker.Service"),
        NEB_PB_ADAPTIVE_SERVICE(1, 28, "com.centreon.broker.AdaptiveService"),
        NEB_PB_SERVICE_STATUS(1, 29, "com.centreon.broker.ServiceStatus"),
        NEB_PB_HOST(1, 30, "com.centreon.broker.Host"),
        NEB_PB_ADAPTIVE_HOST(1, 31, "com.centreon.broker.AdaptiveHost"),
        NEB_PB_HOST_STATUS(1, 32, "com.centreon.broker.HostStatus"),
        NEB_PB_SEVERITY(1, 33, "com.centreon.broker.Severity"),
        NEB_PB_TAG(1, 34, "com.centreon.broker.Tag"),
        NEB_PB_COMMENT(1, 35, "com.centreon.broker.Comment"),
        NEB_PB_DOWNTIME(1, 36, "com.centreon.broker.Downtime"),
        NEB_PB_CUSTOM_VARIABLE(1, 37, "com.centreon.broker.CustomVariable"),
        NEB_PB_CUSTOM_VARIABLE_STATUS(1, 38, "com.centreon.broker.CustomVariableStatus"),
        NEB_PB_HOST_CHECK(1, 39, "com.centreon.broker.HostCheck"),
        NEB_PB_SERVICE_CHECK(1, 40, "com.centreon.broker.ServiceCheck"),
        NEB_PB_LOG_ENTRY(1, 41, "com.centreon.broker.LogEntry"),
        NEB_PB_INSTANCE_STATUS(1, 42, "com.centreon.broker.InstanceStatus"),
        NEB_PB_INSTANCE(1, 44, "com.centreon.broker.Instance"),
        NEB_PB_ACKNOWLEDGEMENT(1, 45, "com.centreon.broker.Acknowledgement"),
        NEB_PB_RESPONSIVE_INSTANCE(1, 46, "com.centreon.broker.ResponsiveInstance"),
        NEB_PB_HOST_GROUP(1, 49, "com.centreon.broker.HostGroup"),
        NEB_PB_HOST_GROUP_MEMBER(1, 50, "com.centreon.broker.HostGroupMember"),
        NEB_PB_SERVICE_GROUP(1, 51, "com.centreon.broker.ServiceGroup"),
        NEB_PB_SERVICE_GROUP_MEMBER(1, 52, "com.centreon.broker.ServiceGroupMember"),
        NEB_PB_HOST_PARENT(1, 53, "com.centreon.broker.HostParent"),
        NEB_PB_INSTANCE_CONFIGURATION(1, 54, "com.centreon.broker.InstanceConfiguration"),
        NEB_PB_ADAPTIVE_SERVICE_STATUS(1, 55, "com.centreon.broker.AdaptiveServiceStatus"),
        NEB_PB_ADAPTIVE_HOST_STATUS(1, 56, "com.centreon.broker.AdaptiveHostStatus"),
        NEB_PB_AGENT_STATS(1, 57, "com.centreon.broker.AgentStats"),
        NEB_PB_UNKNOWN_HOST(1, 58, "com.centreon.broker.UnknownHost"),
        // BBDO category (namespace=2)
        BBDO_PB_ACK(2, 8, "com.centreon.broker.Ack"),
        BBDO_PB_STOP(2, 9, "com.centreon.broker.Stop"),
        BBDO_WELCOME(2, 7, "com.centreon.broker.Welcome"),
        // Storage category (namespace=3), pb_ messages only (de_pb_* from events.hh)
        STORAGE_PB_METRIC(3, 9, "com.centreon.broker.Metric"),
        STORAGE_PB_STATUS(3, 10, "com.centreon.broker.Status"),
        STORAGE_PB_INDEX_MAPPING(3, 11, "com.centreon.broker.IndexMapping"),
        STORAGE_PB_METRIC_MAPPING(3, 12, "com.centreon.broker.MetricMapping"),
        // BAM category (namespace=6), pb_ messages only (de_pb_* from events.hh)
        BAM_PB_INHERITED_DOWNTIME(6, 18, "com.centreon.broker.InheritedDowntime"),
        BAM_PB_BA_STATUS(6, 19, "com.centreon.broker.BaStatus"),
        BAM_PB_BA_EVENT(6, 20, "com.centreon.broker.BaEvent"),
        BAM_PB_KPI_EVENT(6, 21, "com.centreon.broker.KpiEvent"),
        BAM_PB_DIMENSION_BV_EVENT(6, 22, "com.centreon.broker.DimensionBvEvent"),
        BAM_PB_DIMENSION_BA_BV_RELATION_EVENT(6, 23, "com.centreon.broker.DimensionBaBvRelationEvent"),
        BAM_PB_DIMENSION_TIMEPERIOD(6, 24, "com.centreon.broker.DimensionTimeperiod"),
        BAM_PB_DIMENSION_BA_EVENT(6, 25, "com.centreon.broker.DimensionBaEvent"),
        BAM_PB_DIMENSION_KPI_EVENT(6, 26, "com.centreon.broker.DimensionKpiEvent"),
        BAM_PB_KPI_STATUS(6, 27, "com.centreon.broker.KpiStatus"),
        BAM_PB_BA_DURATION_EVENT(6, 28, "com.centreon.broker.BaDurationEvent"),
        BAM_PB_DIMENSION_BA_TIMEPERIOD_RELATION(6, 29, "com.centreon.broker.DimensionBaTimeperiodRelation"),
        BAM_PB_DIMENSION_TRUNCATE_TABLE_SIGNAL(6, 30, "com.centreon.broker.DimensionTruncateTableSignal");

        public final int namespace;
        public final int message;
        public final String messageName;

        BbdoEvent(int namespace, int message, String messageName) {
            this.namespace = namespace;
            this.message = message;
            this.messageName = messageName;
        }

        private static final Map<Integer, BbdoEvent> BY_ID;
        static {
            Map<Integer, BbdoEvent> map = new java.util.HashMap<>();
            for (BbdoEvent e : values()) {
                map.put((e.namespace << 16) | e.message, e);
            }
            BY_ID = Map.copyOf(map);
        }

        public static BbdoEvent fromId(int id) {
            return BY_ID.get(id);
        }

        @Override
        public String toString() {
            return name() + "(" + namespace + "/" + message + ")";
        }
    }

    public record BbdoPacket(BbdoEvent event, int sourceId, int destinationId, Map<String, Object> payload) {

        public ByteBuffer serialize(BinaryCodec decoder) {
            byte[] payloadBytes;
            if (event != null) {
                payloadBytes = decoder.encode(event.messageName, payload);
            } else {
                payloadBytes = (byte[]) payload.get("data");
            }
            int id = event != null ? (event.namespace << 16) | event.message : 0;
            ByteBuffer outBuffer = ByteBuffer.allocate(payloadBytes.length + 16);
            outBuffer.putShort((short)0);
            outBuffer.putShort((short)payloadBytes.length);
            outBuffer.putInt(id);
            outBuffer.putInt(sourceId);
            outBuffer.putInt(destinationId);
            outBuffer.put(payloadBytes);
            outBuffer.putShort(0, (short) calculateCrc(outBuffer));
            return outBuffer.flip().asReadOnlyBuffer();
        }

        private static final int[] CRC16_TABLE = {
                0x0000, 0x1189, 0x2312, 0x329b, 0x4624, 0x57ad, 0x6536, 0x74bf,
                0x8c48, 0x9dc1, 0xaf5a, 0xbed3, 0xca6c, 0xdbe5, 0xe97e, 0xf8f7,
                0x1081, 0x0108, 0x3393, 0x221a, 0x56a5, 0x472c, 0x75b7, 0x643e,
                0x9cc9, 0x8d40, 0xbfdb, 0xae52, 0xdaed, 0xcb64, 0xf9ff, 0xe876,
                0x2102, 0x308b, 0x0210, 0x1399, 0x6726, 0x76af, 0x4434, 0x55bd,
                0xad4a, 0xbcc3, 0x8e58, 0x9fd1, 0xeb6e, 0xfae7, 0xc87c, 0xd9f5,
                0x3183, 0x200a, 0x1291, 0x0318, 0x77a7, 0x662e, 0x54b5, 0x453c,
                0xbdcb, 0xac42, 0x9ed9, 0x8f50, 0xfbef, 0xea66, 0xd8fd, 0xc974,
                0x4204, 0x538d, 0x6116, 0x709f, 0x0420, 0x15a9, 0x2732, 0x36bb,
                0xce4c, 0xdfc5, 0xed5e, 0xfcd7, 0x8868, 0x99e1, 0xab7a, 0xbaf3,
                0x5285, 0x430c, 0x7197, 0x601e, 0x14a1, 0x0528, 0x37b3, 0x263a,
                0xdecd, 0xcf44, 0xfddf, 0xec56, 0x98e9, 0x8960, 0xbbfb, 0xaa72,
                0x6306, 0x728f, 0x4014, 0x519d, 0x2522, 0x34ab, 0x0630, 0x17b9,
                0xef4e, 0xfec7, 0xcc5c, 0xddd5, 0xa96a, 0xb8e3, 0x8a78, 0x9bf1,
                0x7387, 0x620e, 0x5095, 0x411c, 0x35a3, 0x242a, 0x16b1, 0x0738,
                0xffcf, 0xee46, 0xdcdd, 0xcd54, 0xb9eb, 0xa862, 0x9af9, 0x8b70,
                0x8408, 0x9581, 0xa71a, 0xb693, 0xc22c, 0xd3a5, 0xe13e, 0xf0b7,
                0x0840, 0x19c9, 0x2b52, 0x3adb, 0x4e64, 0x5fed, 0x6d76, 0x7cff,
                0x9489, 0x8500, 0xb79b, 0xa612, 0xd2ad, 0xc324, 0xf1bf, 0xe036,
                0x18c1, 0x0948, 0x3bd3, 0x2a5a, 0x5ee5, 0x4f6c, 0x7df7, 0x6c7e,
                0xa50a, 0xb483, 0x8618, 0x9791, 0xe32e, 0xf2a7, 0xc03c, 0xd1b5,
                0x2942, 0x38cb, 0x0a50, 0x1bd9, 0x6f66, 0x7eef, 0x4c74, 0x5dfd,
                0xb58b, 0xa402, 0x9699, 0x8710, 0xf3af, 0xe226, 0xd0bd, 0xc134,
                0x39c3, 0x284a, 0x1ad1, 0x0b58, 0x7fe7, 0x6e6e, 0x5cf5, 0x4d7c,
                0xc60c, 0xd785, 0xe51e, 0xf497, 0x8028, 0x91a1, 0xa33a, 0xb2b3,
                0x4a44, 0x5bcd, 0x6956, 0x78df, 0x0c60, 0x1de9, 0x2f72, 0x3efb,
                0xd68d, 0xc704, 0xf59f, 0xe416, 0x90a9, 0x8120, 0xb3bb, 0xa232,
                0x5ac5, 0x4b4c, 0x79d7, 0x685e, 0x1ce1, 0x0d68, 0x3ff3, 0x2e7a,
                0xe70e, 0xf687, 0xc41c, 0xd595, 0xa12a, 0xb0a3, 0x8238, 0x93b1,
                0x6b46, 0x7acf, 0x4854, 0x59dd, 0x2d62, 0x3ceb, 0x0e70, 0x1ff9,
                0xf78f, 0xe606, 0xd49d, 0xc514, 0xb1ab, 0xa022, 0x92b9, 0x8330,
                0x7bc7, 0x6a4e, 0x58d5, 0x495c, 0x3de3, 0x2c6a, 0x1ef1, 0x0f78,
        };

        private static int calculateCrc(ByteBuffer bb) {
            int crc = 0xFFFF;
            for (int i = 2; i < 16; i++) {
                int c = bb.get(i) & 0xFF;
                crc = ((crc >> 8) & 0xFF) ^ CRC16_TABLE[(crc ^ c) & 0xFF];
            }
            return crc ^ 0xFFFF;
        }

        public static BbdoPacket of(BinaryCodec decoder, ByteBuffer bb) throws IOException {
            int checksum = Short.toUnsignedInt(bb.getShort());
            int size = Short.toUnsignedInt(bb.getShort());
            int id = bb.getInt();
            int sourceId = bb.getInt();
            int destinationId = bb.getInt();
            byte[] data;
            if (size > 0 && size == bb.remaining()) {
                data = new byte[size];
                bb.get(data);
            } else if (size != 0 || size != bb.remaining()) {
                throw new IOException("Invalid BBDO packet size");
            } else {
                data = new byte[0];
            }
            if (checksum != calculateCrc(bb)) {
                throw new IOException("Invalid BBDO packet checksum");
            }
            BbdoEvent event = BbdoEvent.fromId(id);
            Map<String, Object> payload;
            if (event != null) {
                CodedInputStream stream = CodedInputStream.newInstance(data);
                payload = Map.copyOf(decoder.decode(stream, event.messageName, List.of()));
            } else {
                payload = Map.of("data", data);
            }
            return new BbdoPacket(event, sourceId, destinationId, payload);
        }
    }

}
