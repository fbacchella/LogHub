package loghub.sflow.structs;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.netty.buffer.ByteBuf;
import loghub.sflow.DataFormat;
import lombok.Getter;

@Getter
public abstract class Struct {

    @JsonIgnore
    private final DataFormat format;

    protected Struct(DataFormat format) {
        this.format = format;
    }

    protected ByteBuf extractData(ByteBuf buffer) {
        int length = Math.toIntExact(buffer.readUnsignedInt());
        assert length != 0 && length <= buffer.readableBytes() : length;
        int readPosition = buffer.readerIndex();
        buffer.skipBytes(length);
        return buffer.slice(readPosition, length);
    }

    @JsonIgnore
    public abstract String getName();

}
