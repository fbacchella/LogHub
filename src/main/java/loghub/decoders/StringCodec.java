package loghub.decoders;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import loghub.ConnectionContext;
import loghub.Decoder;
import loghub.configuration.Beans;

@Beans({ "charset", "field" })
public class StringCodec extends Decoder {

    private Charset charset = Charset.defaultCharset();
    private String field = "message";

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, byte[] msg, int offset, int length) {
        String message = new String(msg, offset, length, charset);
        return Collections.singletonMap(field, message);
    }

    @Override
    public Map<String, Object> decode(ConnectionContext<?> ctx, ByteBuf bbuf) {
        return Collections.singletonMap(field, bbuf.toString(charset));
    }

    public String getCharset() {
        return charset.name();
    }

    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
