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

    private Object exchange(GrpcReceiver r, GrpcStreamHandler<?, ?> handler, Map<String, Object> message) {
        logger.trace("Received exchange message {}", message);
        if (message.containsKey("buffer")) {
            BbdoPacket inPacket = (BbdoPacket) message.get("buffer");
            BbdoPacket outPacket = switch (inPacket.event()) {
                case BbdoEvent.BBDO_WELCOME -> {
                    Map<String, Object> welcome = new HashMap<>(inPacket.payload());
                    welcome.put("peer_type", "BROKER");
                    welcome.remove("extensions");
                    welcome.put("broker_name", name);
                    yield new BbdoPacket(BbdoEvent.BBDO_WELCOME, inPacket.destinationId(), inPacket.sourceId(), welcome);
                }
                case BbdoEvent.BBDO_PB_STOP -> {
                    int toAck = messageCountAtomic.getAndSet(0);
                    yield new BbdoPacket(BbdoEvent.BBDO_PB_ACK, inPacket.destinationId(), inPacket.sourceId(), Map.of("acknowledged_events", toAck));
                }
                case BbdoEvent.STORAGE_PB_METRIC, BbdoEvent.STORAGE_PB_STATUS -> null;
                case Object o when inPacket.event().category == BbdoCategory.STORAGE -> null;
                default -> {
                    Map<String, Object> buffer = Map.of(
                            "event", inPacket.event(),
                            "destination_id", inPacket.destinationId(),
                            "source_id", inPacket.sourceId(),
                            "data", inPacket.payload()
                    );
                    message.put("buffer", buffer);
                    r.publish(handler, Stream.of(message));
                    yield null;
                }
            };
            if (messageCountAtomic.incrementAndGet() > 1000 && outPacket == null) {
                int toAck = messageCountAtomic.getAndSet(0);
                outPacket = new BbdoPacket(BbdoEvent.BBDO_PB_ACK,
                        inPacket.destinationId(), inPacket.sourceId(), Map.of("acknowledged_events", toAck));
            }
            if (outPacket != null) {
                Object sourceId = message.get("source_id");
                Object destinationId = message.get("destination_id");
                return Map.of("buffer", ByteString.copyFrom(outPacket.serialize(decoder)), "source_id", destinationId, "destination_id", sourceId);
            }
        } else {
            r.publish(handler, Stream.of(message));
        }
        return null;
    }


}
