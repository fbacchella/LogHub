package loghub.sflow.structs;

import io.netty.buffer.ByteBuf;
import loghub.sflow.HeaderProtocol;
import loghub.sflow.SflowParser;
import lombok.Getter;

@Getter
public class SampledHeader extends Struct {

    public static final String NAME = "sampled_header";

    private final HeaderProtocol protocol;
    private final long frame_length;
    private final long stripped;
    private final byte[] header;

    public SampledHeader(SflowParser parser, ByteBuf buf) {
        super(parser.getByName(NAME));
        ByteBuf buffer = extractData(buf);
        protocol = HeaderProtocol.parse(buffer.readInt());
        frame_length = buffer.readUnsignedInt();
        stripped = buffer.readUnsignedInt();
        int length = Math.toIntExact(buffer.readUnsignedInt());
        header = new byte[length];
        buffer.readBytes(header);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String toString() {
        return "SampledHeader(" + "protocol=" + protocol + ", frame_length=" + frame_length + ", stripped=" + stripped + ", header=byte[" + header.length + "])";
    }

}
