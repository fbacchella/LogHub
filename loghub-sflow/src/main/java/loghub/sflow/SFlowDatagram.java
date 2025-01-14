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
    private final InetAddress agent_address;
    private final long sub_agent_id;
    private final long sequence_number;
    private final Duration uptime;
    @JsonIgnore
    private final List<Struct> samples;
}
