package loghub.encoders;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.function.Function;

import loghub.Event;
import loghub.VarFormatter;
import loghub.configuration.Properties;
import loghub.senders.Sender;

public class StringField extends Encoder {

    private Locale locale = Locale.getDefault();
    private Charset charset = Charset.defaultCharset();
    private String format = null;
    private Function<String, byte[]> encoder;
    private VarFormatter formatter;

    @Override
    public boolean configure(Properties properties, Sender sender) {
        // With UTF-8, String.getBytes(String) is faster than String.getBytes(Charset), by about 10%
        // See https://issues.apache.org/jira/browse/LOG4J2-935
        // See also http://psy-lob-saw.blogspot.fr/2012/12/encode-utf-8-string-to-bytebuffer-faster.html
        if ("UTF-8".equals(charset.name())) {
            encoder = i -> {
                try {
                    return i.getBytes("UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            encoder = i -> i.getBytes(charset);
        }
        try {
            formatter = new VarFormatter(format);
        } catch (IllegalArgumentException e) {
            logger.error("Illegal format \"{}\": {}", format, e.getMessage());
            return false;
        }
        return super.configure(properties, sender);
    }

    @Override
    public byte[] encode(Event event) {
        return encoder.apply(formatter.format(event));
    }

    public String getCharset() {
        return charset.name();
    }

    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

    public String getLocale() {
        return locale.toLanguageTag();
    }

    public void setLocale(String local) {
        this.locale = Locale.forLanguageTag(local);
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

}
