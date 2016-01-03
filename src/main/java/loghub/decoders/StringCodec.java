package loghub.decoders;

import java.nio.charset.Charset;

import loghub.Decode;
import loghub.Event;
import loghub.configuration.Beans;

@Beans({"charset", "field"})
public class StringCodec extends Decode {

    private Charset charset = Charset.defaultCharset();
    private String field = "message";

    @Override
    public void decode(Event event, byte[] msg, int offset, int length) {
        String message = new String(msg, offset, length, charset);
        event.put(field, message);
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
