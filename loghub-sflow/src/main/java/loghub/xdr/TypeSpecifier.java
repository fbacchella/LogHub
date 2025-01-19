package loghub.xdr;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import lombok.Getter;

@Getter
public abstract class TypeSpecifier<T> {

    private final String name;
    protected TypeSpecifier(String name) {
        this.name = name;
    }
    public abstract T getType();
    public abstract <O> O read(ByteBuf b) throws IOException;

}
