package loghub.sflow.structs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.sflow.DataFormat;
import loghub.sflow.SflowParser;

public class DynamicStruct extends Struct {

    private final Map<String, Object> data;

    public DynamicStruct(String name, SflowParser parser, ByteBuf buf) throws IOException {
        super(parser.getByName(name));
        buf = extractData(buf);
        data = parser.readDynamicStructData(getFormat(), buf);
    }

    @Override
    public String getName() {
        return getFormat().getName();
    }
}
