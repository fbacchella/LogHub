package loghub.xdr;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class IndirectTypeSpecifier<TS> extends TypeSpecifier<TypeSpecifier<TS>> {

    private final TypeSpecifier<TS> type;

    protected IndirectTypeSpecifier(String name, TypeSpecifier<TS> type) {
        super(name);
        this.type = type;
    }

    @Override
    public TypeSpecifier<TS> getType() {
        return type;
    }

    @Override
    public <O> O read(ByteBuf b) throws IOException {
        return type.read(b);
    }

    @Override
    public String toString() {
        return '(' + getName() + "->" + type.toString() + ')';
    }
}
