package loghub.processors;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;

/**
 * Implementations of the CEF specifications, see
 * <a href="https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors-8.4/pdfdoc/cef-implementation-standard/cef-implementation-standard.pdf">Micro Focus Security ArcSight Common Event Format</a>.
 * <p>
 * They are totally brain-dead, many corner case and missing escape missing: single | or &lt;space&gt; are allowed in extensions.
 *
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(ParseCef.Builder.class)
@FieldsProcessor.InPlace
public class ParseCef extends FieldsProcessor {

    private static final String[] COLUMNS = new String[]{"version", "device_vendor", "device_product", "device_version", "device_event_class_id", "name", "severity"};
    private static final Pattern fieldPattern = Pattern.compile("(?:(\\d+)|(?:[^|]|(?<=\\\\)\\|)+)(?=\\|)");
    private static final Pattern extensionPattern = Pattern.compile("(?<key>\\w+)=(?<value>(?:[^ ]| (?! *\\w+=))+)");
    private static final ThreadLocal<Matcher> fieldMatcherSource = ThreadLocal.withInitial(() -> fieldPattern.matcher(""));
    private static final ThreadLocal<Matcher> extensionMatcherSource = ThreadLocal.withInitial(() -> extensionPattern.matcher(""));

    public static class Builder extends FieldsProcessor.Builder<ParseCef> {
        public ParseCef build() {
            return new ParseCef(this);
        }
    }
    public static ParseCef.Builder getBuilder() {
        return new ParseCef.Builder();
    }

    public ParseCef(ParseCef.Builder builder) {
        super(builder);
    }

    @Override
    public Object fieldFunction(Event event, Object value)
            throws ProcessorException {
        String message = value.toString();
        if (! message.startsWith("CEF:")) {
            throw event.buildException("not a CEF message: \"" + message + "\"");
        }
        Matcher m = fieldMatcherSource.get();
        Matcher m2 = extensionMatcherSource.get();
        try {
            Map<String, Object> cefContent = new HashMap<>(COLUMNS.length);
            // The CEF specifications are totally brain-dead, so nothing is easy, needs to be parsed with care
            m.reset(message);
            m.region(4, message.length());

            for (String column : COLUMNS) {
                if (!m.find()) {
                    throw event.buildException("Not a valid CEF message");
                } else {
                    if (m.group(1) != null && ("version".equals(column) || "severity".equals(column))) {
                        cefContent.put(column, Integer.valueOf(m.group(1)));
                    } else {
                        cefContent.put(column, m.group().replace("\\\\", "\\").replace("\\|", "|"));
                    }
                }
            }
            String extension = message.substring(m.end() + 1);

            //Resolution of extensions
            Map<String, Object> cefExtensions = new HashMap<>();
            m2.reset(extension);
            while (m2.find()) {
                cefExtensions.put(m2.group("key"), m2.group("value").replace("\\\\", "\\").replace("\\=", "=").replace("\\n", "\n").replace("\\r", "\r"));
            }
            cefContent.put("extensions", cefExtensions);
            return cefContent;
        } finally {
            m.reset();
            m2.reset();
        }
    }

}
