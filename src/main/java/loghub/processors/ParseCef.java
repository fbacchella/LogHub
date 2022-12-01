package loghub.processors;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

/**
 * Implementations of the CEF specifications, see https://kc.mcafee.com/resources/sites/MCAFEE/content/live/CORP_KNOWLEDGEBASE/78000/KB78712/en_US/CEF_White_Paper_20100722.pdf.
 * <p>
 * They are totally brain dead, many corner case and missing escape missing: single | or &lt;space&gt; are allowed in extensions.
 * 
 * @author Fabrice Bacchella
 *
 */
public class ParseCef extends Processor {

    private static final String[] COLUMNS = new String[]{"version", "device_vendor", "device_product", "device_version", "device_event_class_id", "name", "severity"};
    private static final Pattern fieldPattern = Pattern.compile("(?:[^\\|]|(?<=\\\\)\\|)+");
    private static final Pattern extensionPattern = Pattern.compile("(?<key>[a-zA-Z0-9_]+)=(?<value>(?:[^ ]| (?! *[a-zA-Z0-9_]+=))+)");

    @Getter @Setter
    private VariablePath field = VariablePath.of("message");
    private ThreadLocal<Matcher> fieldMatcherSource = ThreadLocal.withInitial(() -> fieldPattern.matcher(""));
    private ThreadLocal<Matcher> extensionMatcherSource = ThreadLocal.withInitial(() -> extensionPattern.matcher(""));

    @Override
    public boolean process(Event event) throws ProcessorException {
        String message = event.getAtPath(field).toString();
        if (! message.startsWith("CEF:")) {
            throw event.buildException("not a CEF message: \"" + message + "\"");
        }
        message = message.substring(4);
        try {
            Map<String, Object> cefContent = new HashMap<>(COLUMNS.length);
            // The CEF specifications are totally brain-dead, so nothing is easy, needs to be parsed with care
            Matcher m = fieldMatcherSource.get();
            m.reset(message);

            // First resolution of the CEF fields
            int i = -1;
            while (++i < 7 && m.find()) {
                String column = COLUMNS[i];
                switch (column) {
                case "version":
                case "severity":
                    try {
                        cefContent.put(column, Integer.valueOf(m.group()));
                    } catch (Exception e) {
                        cefContent.put(column + "_failed", m.group());
                    }
                    break;
                default:
                    cefContent.put(column, m.group().replace("\\\\", "\\").replace("\\|", "|"));
                }
            }
            String extension = message.substring(m.end() + 1);

            //Resolution of extensions
            Map<String, Object> cefExtensions = new HashMap<>();
            Matcher m2 = extensionMatcherSource.get();
            m2.reset(extension);
            while (m2.find()) {
                cefExtensions.put(m2.group("key"), m2.group("value").replace("\\\\", "\\").replace("\\=", "=").replace("\\n", "\n").replace("\\r", "\r"));
            }

            event.put("cef_fields", cefContent);
            event.put("cef_extensions", cefExtensions);

            return true;
        } catch (Exception e) {
            throw event.buildException("failed to parse CEF: " + message, e);
        }
    }

}
