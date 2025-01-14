package loghub.sflow.structs;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class LagPortStats extends OpaqueStruct {

    public static final String NAME = "lag_port_stats";

    public LagPortStats(SflowParser df, ByteBuf buf) {
        super(df.getByName(NAME), buf);
    }

    @Override
    public String getName() {
        return NAME;
    }

}
