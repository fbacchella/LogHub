package loghub.encoders;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Locale;
import java.util.function.Function;

import loghub.BuilderClass;
import loghub.Event;
import loghub.VarFormatter;
import loghub.configuration.Properties;
import loghub.senders.Sender;
import lombok.Setter;

@BuilderClass(StringField.Builder.class)
public class StringField extends Encoder {

    public static class Builder extends Encoder.Builder<StringField> {
        @Setter
        private String locale = Locale.getDefault().toLanguageTag();
        @Setter
        private String charset = Charset.defaultCharset().name();
        @Setter
        private String format = null;
        @Setter
        private Function<String, byte[]> encoder;
        @Setter
        private VarFormatter formatter;
        @Override
        public StringField build() {
            return new StringField(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    private final Function<String, byte[]> encoder;
    private VarFormatter formatter;

    private StringField(Builder builder) {
        super(builder);
        Locale locale = Locale.forLanguageTag(builder.locale);
        Charset charset = Charset.forName(builder.charset);
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
            formatter = new VarFormatter(builder.format, locale);
        } catch (IllegalArgumentException e) {
            logger.error("Illegal format \"{}\": {}", builder.format, e.getMessage());
            formatter = null;
        }
    }

    @Override
    public boolean configure(Properties properties, Sender sender) {
        if (formatter == null) {
            return false;
        }
        return super.configure(properties, sender);
    }

    @Override
    public byte[] encode(Event event) {
        return encoder.apply(formatter.format(event));
    }

}
