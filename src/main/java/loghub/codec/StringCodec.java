package loghub.codec;

import java.nio.charset.Charset;

import loghub.Codec;
import loghub.Event;
import loghub.configuration.Beans;

@Beans({"charset"})
public class StringCodec extends Codec {

    private Charset charset = Charset.defaultCharset();

    @Override
    public void decode(Event event, byte[] msg) {
        String message = new String(msg, charset);
        event.put("message", message);
    }

    public String getCharset() {
        return charset.name();
    }

    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

}
