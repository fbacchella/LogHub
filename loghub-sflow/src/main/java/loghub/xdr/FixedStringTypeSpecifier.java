package loghub.xdr;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class FixedStringTypeSpecifier extends TypeSpecifier<NativeType> {

    private final int size;

    public FixedStringTypeSpecifier(int size) {
        super(NativeType.BYTE_ARRAY.typeName);
        this.size = size;
    }

    @Override
    public NativeType getType() {
        return null;
    }

    @Override
    public <O> O read(ByteBuf b) throws IOException {
        byte[] data = new byte[size];
        b.readBytes(data);
        return (O) data;
    }
}
