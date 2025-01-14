package loghub.sflow.structs;

import java.util.ArrayList;
import java.util.List;

import loghub.sflow.StructureClass;
import loghub.sflow.SflowParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class FlowSample extends Struct {

    public static final String NAME = "flow_sample";
    private final long sequenceNumber;
    private final long samplingRate;
    private final long samplePool;
    private final long drops;
    private final List<Struct> samples = new ArrayList<>();

    public FlowSample(SflowParser df, ByteBuf buf) {
        super(df.getByName(NAME));
        buf = extractData(buf);
        sequenceNumber = buf.readUnsignedInt();
        buf.readUnsignedInt();
        samplingRate = buf.readUnsignedInt();
        samplePool = buf.readUnsignedInt();
        drops = buf.readUnsignedInt();
        buf.readUnsignedInt(); // interface input
        buf.readUnsignedInt(); // interface output
        long flowRecords = buf.readUnsignedInt();
        for (int i = 0; i < flowRecords; i++) {
            samples.add(df.readStruct(StructureClass.FLOW_DATA, buf));
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

}
