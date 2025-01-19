package loghub.xdr;

import java.io.IOException;

import io.netty.buffer.ByteBuf;

public interface ReadType<T> {
    T read(ByteBuf buf) throws IOException;
}
