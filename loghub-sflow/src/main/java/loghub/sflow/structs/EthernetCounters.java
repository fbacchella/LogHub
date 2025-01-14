package loghub.sflow.structs;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class EthernetCounters extends Struct {

    public static final String NAME = "ethernet_counters";

    private final long dot3StatsAlignmentErrors;
    private final long dot3StatsFCSErrors;
    private final long dot3StatsSingleCollisionFrames;
    private final long dot3StatsMultipleCollisionFrames;
    private final long dot3StatsSQETestErrors;
    private final long dot3StatsDeferredTransmissions;
    private final long dot3StatsLateCollisions;
    private final long dot3StatsExcessiveCollisions;
    private final long dot3StatsInternalMacTransmitErrors;
    private final long dot3StatsCarrierSenseErrors;
    private final long dot3StatsFrameTooLongs;
    private final long dot3StatsInternalMacReceiveErrors;
    private final long dot3StatsSymbolErrors;

    public EthernetCounters(SflowParser df, ByteBuf buffer) {
        super(df.getByName(NAME));
        buffer = extractData(buffer);

        dot3StatsAlignmentErrors = buffer.readUnsignedInt();
        dot3StatsFCSErrors = buffer.readUnsignedInt();
        dot3StatsSingleCollisionFrames = buffer.readUnsignedInt();
        dot3StatsMultipleCollisionFrames = buffer.readUnsignedInt();
        dot3StatsSQETestErrors = buffer.readUnsignedInt();
        dot3StatsDeferredTransmissions = buffer.readUnsignedInt();
        dot3StatsLateCollisions = buffer.readUnsignedInt();
        dot3StatsExcessiveCollisions = buffer.readUnsignedInt();
        dot3StatsInternalMacTransmitErrors = buffer.readUnsignedInt();
        dot3StatsCarrierSenseErrors = buffer.readUnsignedInt();
        dot3StatsFrameTooLongs = buffer.readUnsignedInt();
        dot3StatsInternalMacReceiveErrors = buffer.readUnsignedInt();
        dot3StatsSymbolErrors = buffer.readUnsignedInt();
    }

    @Override
    public String getName() {
        return NAME;
    }

}
