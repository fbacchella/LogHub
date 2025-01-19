package loghub.xdr;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public class InvalidTypeSpecifier extends TypeSpecifier<String> {

    String typeName;
    protected InvalidTypeSpecifier(String name, String typeName) {
        super(name);
        this.typeName = typeName;
    }

    @Override
    public String getType() {
        return "InvalidType/" + getName();
    }

    @Override
    public <O> O read(ByteBuf b) throws IOException {
        throw new IOException("Unspecified type '" + getName() + "', not readable for declaration '" + typeName + "'");
    }
}
