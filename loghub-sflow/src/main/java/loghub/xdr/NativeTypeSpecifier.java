package loghub.xdr;

import java.io.IOException;
import java.util.Objects;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public class NativeTypeSpecifier extends TypeSpecifier<NativeType> {

    private final NativeType type;
    int size;

    protected NativeTypeSpecifier(NativeType type, int size) {
        super(type.typeName);
        this.type = type;
        this.size = size;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof NativeTypeSpecifier))
            return false;
        NativeTypeSpecifier that = (NativeTypeSpecifier) o;
        return type == that.type && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, size);
    }

    @Override
    public String toString() {
        return type.getString(size);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O> O read(ByteBuf b) throws IOException {
        return (O) type.reader.read(b);
    }
}
