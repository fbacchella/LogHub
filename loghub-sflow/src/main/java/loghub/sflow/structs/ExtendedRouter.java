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
    private final long src_mask_len;
    private final long dst_mask_len;

    public ExtendedRouter(SflowParser df, ByteBuf buffer) {
        super(df.getByName(NAME));
        try {
            buffer = extractData(buffer);
            nexthop = df.readIpAddress(buffer);
            src_mask_len = buffer.readUnsignedInt();
            dst_mask_len = buffer.readUnsignedInt();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

}
