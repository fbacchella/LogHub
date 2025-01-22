package loghub.sflow.structs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;
import loghub.types.MacAddress;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class LagPortStats extends Struct {

    public static final String NAME = "lag_port_stats";

    private final MacAddress dot3adAggPortActorSystemID;
    private final MacAddress dot3adAggPortPartnerOperSystemID;
    private final long dot3adAggPortAttachedAggID;
    private final Map<String, Set<String>> dot3adAggPortState;
    private final long dot3adAggPortStatsLACPDUsRx;
    private final long dot3adAggPortStatsMarkerPDUsRx;
    private final long dot3adAggPortStatsMarkerResponsePDUsRx;
    private final long dot3adAggPortStatsUnknownRx;
    private final long dot3adAggPortStatsIllegalRx;
    private final long dot3adAggPortStatsLACPDUsTx;
    private final long dot3adAggPortStatsMarkerPDUsTx;
    private final long dot3adAggPortStatsMarkerResponsePDUsTx;

    public LagPortStats(SflowParser parser, ByteBuf buf) throws IOException {
        super(parser.getByName(NAME));
        buf = extractData(buf);

        // MAC address can be 6 or 8 bytes. As this is not explicit, the value must be inferred
        int macLength = buf.readableBytes() == 52 ? 6 : 8;
        byte[] macBuffer = new byte[macLength];
        buf.readBytes(macBuffer);
        dot3adAggPortActorSystemID = new MacAddress(macBuffer);
        buf.readBytes(macBuffer);
        dot3adAggPortPartnerOperSystemID = new MacAddress(macBuffer);

        dot3adAggPortAttachedAggID = buf.readUnsignedInt();
        byte[] dot3adAggPortStateBuffer = new byte[4];
        buf.readBytes(dot3adAggPortStateBuffer);
        dot3adAggPortState = Map.of(
                "dot3adAggPortActorAdminState", decodePortStatus(dot3adAggPortStateBuffer[0]),
                "dot3adAggPortActorOperState", decodePortStatus(dot3adAggPortStateBuffer[1]),
                "dot3adAggPortPartnerAdminState", decodePortStatus(dot3adAggPortStateBuffer[2]),
                "dot3adAggPortPartnerOperState", decodePortStatus(dot3adAggPortStateBuffer[3])
        );
        dot3adAggPortStatsLACPDUsRx = buf.readUnsignedInt();
        dot3adAggPortStatsMarkerPDUsRx = buf.readUnsignedInt();
        dot3adAggPortStatsMarkerResponsePDUsRx = buf.readUnsignedInt();
        dot3adAggPortStatsUnknownRx = buf.readUnsignedInt();
        dot3adAggPortStatsIllegalRx = buf.readUnsignedInt();
        dot3adAggPortStatsLACPDUsTx = buf.readUnsignedInt();
        dot3adAggPortStatsMarkerPDUsTx = buf.readUnsignedInt();
        dot3adAggPortStatsMarkerResponsePDUsTx = buf.readUnsignedInt();
    }

    private Set<String> decodePortStatus(byte portStatusEncoded) {
        Set<String> portStatus = new HashSet<>();
        if ((portStatusEncoded & 1) != 0) {
            portStatus.add("lacpActivity");
        }
        if ((portStatusEncoded & 1 << 1) != 0) {
            portStatus.add("lacpTimeout");
        }
        if ((portStatusEncoded & 1 << 2) != 0) {
            portStatus.add("aggregation");
        }
        if ((portStatusEncoded & 1 << 3) != 0) {
            portStatus.add("synchronization");
        }
        if ((portStatusEncoded & 1 << 4) != 0) {
            portStatus.add("collecting");
        }
        if ((portStatusEncoded & 1 << 5) != 0) {
            portStatus.add("distributing");
        }
        if ((portStatusEncoded & 1 << 6) != 0) {
            portStatus.add("defaulted");
        }
        if ((portStatusEncoded & 1 << 7) != 0) {
            portStatus.add("expired");
        }

        return portStatus;
    }

    @Override
    public String getName() {
        return NAME;
    }

}
