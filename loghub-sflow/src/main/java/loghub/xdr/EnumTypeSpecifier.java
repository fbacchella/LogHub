package loghub.xdr;

import java.util.Map;

import io.netty.buffer.ByteBuf;

public class EnumTypeSpecifier extends TypeSpecifier<Map<Number, String>> {

    private final Map<Number, String> enumEntries;

    EnumTypeSpecifier(String name, Map<Number, String> enumEntries) {
        super(name);
        this.enumEntries = Map.copyOf(enumEntries);
    }

    public String resolve(Number n) {
        return enumEntries.get(n);
    }

    @Override
    public Map<Number, String> getType() {
        return enumEntries;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String read(ByteBuf b) {
        return enumEntries.get(b.readInt());
    }

    @Override
    public String toString() {
        return "enum%s" + enumEntries;
    }

}
