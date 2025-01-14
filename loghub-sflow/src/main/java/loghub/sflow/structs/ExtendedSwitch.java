package loghub.sflow.structs;

import loghub.sflow.SflowParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class ExtendedSwitch extends Struct {

    public static final String NAME = "extended_switch";

    private final long src_vlan;
    private final long src_priority;
    private final long dst_vlan;
    private final long dst_priority;

    public ExtendedSwitch(SflowParser df, ByteBuf buf) {
        super(df.getByName(NAME));
        buf = extractData(buf);
        src_vlan = buf.readUnsignedInt();
        src_priority = buf.readUnsignedInt();
        dst_vlan = buf.readUnsignedInt();
        dst_priority = buf.readUnsignedInt();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
