package loghub.sflow.structs;

import java.nio.charset.Charset;

import io.netty.buffer.ByteBuf;
import loghub.sflow.SflowParser;
import lombok.Getter;

@Getter
public class ExtendedUser extends Struct {

    public static final String NAME = "extended_user";

    private final String src_user;
    private final String dst_user;

    public ExtendedUser(SflowParser parser, ByteBuf buf) {
        super(parser.getByName(NAME));
        buf = extractData(buf);
        Charset src_user_charset = parser.readCharset(buf);
        int src_user_len = Math.toIntExact(buf.readUnsignedInt());
        byte[] src_user_bytes = new byte[src_user_len];
        buf.readBytes(src_user_bytes);
        src_user = new String(src_user_bytes, src_user_charset);
        Charset dst_user_charset = parser.readCharset(buf);
        int dst_user_len = Math.toIntExact(buf.readUnsignedInt());
        byte[] dst_user_bytes = new byte[dst_user_len];
        buf.readBytes(dst_user_bytes);
        dst_user = new String(dst_user_bytes, dst_user_charset);
    }

    @Override
    public String getName() {
        return NAME;
    }
}
