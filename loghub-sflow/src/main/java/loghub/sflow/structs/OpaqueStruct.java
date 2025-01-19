package loghub.sflow.structs;

import loghub.sflow.DataFormat;
import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class OpaqueStruct extends Struct {

    private final ByteBuf data;
    public OpaqueStruct(DataFormat si, ByteBuf data) {
        super(si);
        this.data = extractData(data).copy();
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", getName(), getFormat());
    }

    @Override
    public String getName() {
        return "UnknownStruct(" + getFormat() + ")";
    }

}
