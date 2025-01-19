package loghub.sflow.structs;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;

@JsonSerialize(using = DynamicStruct.DynamicStructSerializer.class)
public class DynamicStruct extends Struct {

    static public class DynamicStructSerializer extends JsonSerializer<DynamicStruct> {
        @Override
        public void serialize(DynamicStruct value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeObject(value.data);
        }
    }

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
