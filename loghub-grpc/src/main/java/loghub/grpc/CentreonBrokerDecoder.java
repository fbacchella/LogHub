package loghub.grpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;

import loghub.NullOrMissingValue;

public class CentreonBrokerDecoder extends BinaryCodec {
    public CentreonBrokerDecoder() throws DescriptorValidationException, IOException {
        super("Centreon", CentreonBrokerDecoder.class.getClassLoader().getResourceAsStream("centreon.binpb"));
    }

    @Override
    protected void initFastPath() {
        super.initFastPath();
        addFastPath("com.centreon.broker.stream.CentreonEvent.buffer", this::resolveBuffer);
        // Service
        addFastPath("com.centreon.broker.Service.last_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_hard_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_notification", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_time_ok", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_time_warning", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_time_critical", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_time_unknown", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.last_update", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.next_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Service.next_notification", this::resolveTimestamp);
        // AdaptiveServiceStatus
        addFastPath("com.centreon.broker.AdaptiveServiceStatus.next_check", this::resolveTimestamp);
        // ServiceCheck
        addFastPath("com.centreon.broker.ServiceCheck.next_check", this::resolveTimestamp);
        // ServiceStatus
        addFastPath("com.centreon.broker.ServiceStatus.last_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_hard_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_time_ok", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_time_warning", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_time_critical", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_time_unknown", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.next_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.last_notification", this::resolveTimestamp);
        addFastPath("com.centreon.broker.ServiceStatus.next_notification", this::resolveTimestamp);
        // Host
        addFastPath("com.centreon.broker.Host.last_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_hard_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_notification", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_time_down", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_time_unreachable", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_time_up", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.last_update", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.next_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Host.next_host_notification", this::resolveTimestamp);
        // AdaptiveHostStatus
        addFastPath("com.centreon.broker.AdaptiveHostStatus.next_check", this::resolveTimestamp);
        // HostCheck
        addFastPath("com.centreon.broker.HostCheck.next_check", this::resolveTimestamp);
        // HostStatus
        addFastPath("com.centreon.broker.HostStatus.last_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_hard_state_change", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_time_up", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_time_down", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_time_unreachable", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.next_check", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.last_notification", this::resolveTimestamp);
        addFastPath("com.centreon.broker.HostStatus.next_host_notification", this::resolveTimestamp);
        // LogEntry
        addFastPath("com.centreon.broker.LogEntry.ctime", this::resolveTimestamp);
        // Downtime
        addFastPath("com.centreon.broker.Downtime.entry_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Downtime.actual_start_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Downtime.actual_end_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Downtime.start_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Downtime.deletion_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Downtime.end_time", this::resolveTimestamp);
        // Acknowledgement
        addFastPath("com.centreon.broker.Acknowledgement.entry_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Acknowledgement.deletion_time", this::resolveTimestamp);
        // Comment
        addFastPath("com.centreon.broker.Comment.deletion_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Comment.entry_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Comment.expire_time", this::resolveTimestamp);
        // CustomVariable
        addFastPath("com.centreon.broker.CustomVariable.update_time", this::resolveTimestamp);
        // CustomVariableStatus
        addFastPath("com.centreon.broker.CustomVariableStatus.update_time", this::resolveTimestamp);
        // Instance
        addFastPath("com.centreon.broker.Instance.end_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Instance.start_time", this::resolveTimestamp);
        addFastPath("com.centreon.broker.Instance.cma_cert_peremption", this::resolveTimestamp);
        // InstanceStatus
        addFastPath("com.centreon.broker.InstanceStatus.last_alive", this::resolveTimestamp);
        addFastPath("com.centreon.broker.InstanceStatus.last_command_check", this::resolveTimestamp);

        addFastPath("com.centreon.broker.Metric.time", this::resolveTimestamp);

    }

    private Object resolveTimestamp(
            CodedInputStream codedInputStream, FieldDescriptor fieldDescriptor, List<UnknownField> unknownFields)
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
        return BbdoPacket.of(this, bb);
    }
}
