package loghub.encoders;

import java.nio.charset.Charset;
import java.util.Formatter;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.Encoder;
import loghub.Event;
import loghub.configuration.Beans;

@Beans({"locale", "charset", "format"})
public class StringField extends Encoder {

    private static final Pattern varregexp = Pattern.compile("(?<before>.*?)(?:\\$\\{(?<varname>[\\w\\.-]+)(?<format>%[^}]+)?\\})(?<after>.*)");

    private Locale locale = Locale.getDefault();
    private Charset charset = Charset.defaultCharset();
    private String format = null;

    private StringBuilder findVariables(Formatter f, StringBuilder buffer, String in, Map<String, Object> env) {
        Matcher m = varregexp.matcher(in);
        if(m.find()) {
            String before = m.group("before");
            String format = m.group("format");
            String varname = m.group("varname");
            String after = m.group("after");
            buffer.append(before);
            if(varname == null || ! varname.isEmpty()) {
                if(format == null || format.isEmpty()) {
                    format = "%s";
                }
                f.format(format, env.get(varname));

            }
            findVariables(f, buffer, after, env);
        }
        return buffer;
    }

    @Override
    public byte[] encode(Event event) {
        StringBuilder buffer = new StringBuilder();
        findVariables(new Formatter(buffer, locale), buffer, format, event);
        return buffer.toString().getBytes(charset);
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
