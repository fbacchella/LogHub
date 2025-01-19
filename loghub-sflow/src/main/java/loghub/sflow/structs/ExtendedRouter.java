package loghub.sflow.structs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;

import loghub.sflow.SflowParser;
import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class ExtendedRouter extends Struct {

    public static final String NAME = "extended_router";

    private final InetAddress nexthop;
    private final int src_mask_len;
    private final int dst_mask_len;

    public ExtendedRouter(SflowParser parser, ByteBuf buffer) throws IOException {
        super(parser.getByName(NAME));
        buffer = extractData(buffer);
        nexthop = parser.readIpAddress(buffer);
        src_mask_len = Math.toIntExact(buffer.readUnsignedInt());
        dst_mask_len = Math.toIntExact(buffer.readUnsignedInt());
    }

    @Override
    public String getName() {
        return NAME;
    }

}
