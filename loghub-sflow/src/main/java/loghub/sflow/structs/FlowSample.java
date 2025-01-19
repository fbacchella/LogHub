package loghub.sflow.structs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;
import loghub.sflow.StructureClass;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class FlowSample extends Struct {

    public static final String NAME = "flow_sample";
    private final long sequenceNumber;
    private final Map<String, Object> source_id;
    private final long samplingRate;
    private final long samplePool;
    private final long drops;
    private final Map<String, Object> input;
    private final Map<String, Object> ouput;
    private final List<Map.Entry<String, Struct>> samples = new ArrayList<>();

    public FlowSample(SflowParser parser, ByteBuf buf) throws IOException {
        super(parser.getByName(NAME));
        buf = extractData(buf);
        sequenceNumber = buf.readUnsignedInt();
        source_id = parser.readDataSource(buf);
        samplingRate = buf.readUnsignedInt();
        samplePool = buf.readUnsignedInt();
        drops = buf.readUnsignedInt();
        input = readInterface(buf);
        ouput = readInterface(buf); // interface output
        long flowRecords = buf.readUnsignedInt();
        for (int i = 0; i < flowRecords; i++) {
            Struct s = parser.readStruct(StructureClass.FLOW_DATA, buf);
            samples.add(Map.entry(s.getName(), s));
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Map<String, Object> readInterface(ByteBuf buf) {
        Map<String, Object> values = new HashMap<>();
        int interfaceCode = buf.readInt();
        int mode = interfaceCode >>> 30;
        switch (mode) {
        case 0:
            values.put("ifIndex", interfaceCode & ((2 << 29) - 1));
            break;
        case 1:
            values.put("droppedReason", resolveDropReaser(interfaceCode & ((2 << 29) - 1)));
            break;
        case 3:
            values.put("numInterfaces", interfaceCode & ((2 << 29) - 1));
            break;
        }
        return values;
    }

    private String resolveDropReaser(int code) {
        switch (code) {
        case 0:   return "Net Unreachable";
        case 1:   return "Host Unreachable";
        case 2:   return "Protocol Unreachable";
        case 3:   return "Port Unreachable";
        case 4:   return "Fragmentation Needed and Don't Fragment was Set";
        case 5:   return "Source Route Failed";
        case 6:   return "Destination Network Unknown";
        case 7:   return "Destination Host Unknown";
        case 8:   return "Source Host Isolated";
        case 9:   return "Communication with Destination Network is Administratively Prohibited";
        case 10:  return "Communication with Destination Host is Administratively Prohibited";
        case 11:  return "Destination Network Unreachable for Type of Service";
        case 12:  return "Destination Host Unreachable for Type of Service";
        case 13:  return "Communication Administratively Prohibited";
        case 14:  return "Host Precedence Violation";
        case 15:  return "Precedence cutoff in effect";
        case 256: return "Unknown";
        case 257 :return "TTL exceeded";
        case 258: return "ACL";
        case 259: return "No buffer space";
        case 260: return "RED";
        case 261: return "Traffic shaping/rate limiting";
        case 262: return "Packet too big";
        default:  return "Unidentified";
        }
    }

}
