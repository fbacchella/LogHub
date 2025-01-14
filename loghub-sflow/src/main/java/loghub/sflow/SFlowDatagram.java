package loghub.sflow;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import loghub.sflow.structs.Struct;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
@Getter
public class SFlowDatagram {
    private final int version;
    private final InetAddress agentAddress;
    private final long subAgentId;
    private final long sequenceNumber;
    private final Duration uptime;
    @JsonIgnore
    private final List<Struct> samples;
}
