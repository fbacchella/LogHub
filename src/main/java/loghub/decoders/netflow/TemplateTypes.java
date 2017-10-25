package loghub.decoders.netflow;

import io.netty.buffer.ByteBuf;

public interface TemplateTypes {
    public Object getValue(int i, ByteBuf bbuf);
    public String getName(int i);
}
