package loghub.xdr;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class FixedOpaqueTypeSpecifier extends TypeSpecifier<NativeType> {

    private final int size;

    public FixedOpaqueTypeSpecifier(int size) {
        super(NativeType.BYTE_ARRAY.typeName);
        this.size = size;
    }

    @Override
    public NativeType getType() {
        return null;
    }

    @Override
    public <O> O read(ByteBuf b) {
        byte[] data = new byte[size];
        b.readBytes(data);
        return (O) data;
    }

}
