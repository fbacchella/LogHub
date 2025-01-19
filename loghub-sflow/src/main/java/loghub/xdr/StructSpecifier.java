package loghub.xdr;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import io.netty.buffer.ByteBuf;

public class StructSpecifier extends TypeSpecifier<Map<String, TypeSpecifier<?>>> {
    public interface CustomReader {
        Object read(String name, TypeSpecifier<?> type, ByteBuf buf) throws IOException;
    }
    private final Map<String, TypeSpecifier<?>> structDescription;

    StructSpecifier(String name, LinkedHashMap<String, TypeSpecifier<?>> structDescription) {
        super(name);
        this.structDescription = structDescription;
    }

    @Override
    public Map<String, TypeSpecifier<?>> getType() {
        return structDescription;
    }

    @Override
    public Map<String, Object> read(ByteBuf b) throws IOException {
        Map<String, Object> data = new HashMap<>();
        for (Map.Entry<String, TypeSpecifier<?>> e: structDescription.entrySet()) {
            data.put(e.getKey(), e.getValue().read(b));
        }
        return data;
    }

    public Map<String, Object> read(ByteBuf b, CustomReader reader) throws IOException {
        Map<String, Object> data = new HashMap<>();
        for (Map.Entry<String, TypeSpecifier<?>> e: structDescription.entrySet()) {
            data.put(e.getKey(), reader.read(e.getKey(), e.getValue(), b));
        }
        return data;
    }


    @Override
    public String toString() {
        return "struct" + structDescription;
    }

}
