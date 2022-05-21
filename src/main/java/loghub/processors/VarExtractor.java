package loghub.processors;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.Event;

/**
 * This transformer parse a field using a regex that extract name and value.
 * The regex must contains two named group name and value. The field is parsed until exhaustion. The unmatched content will stay in the field
 * unless everything match, in this case, the field is removed.
 * <p>
 * The default parser is "(?&lt;name&gt;\p{Alnum}+)\p{Space}?[=:]\p{Space}?(?&lt;value&gt;[^;,:]+)[;,:]?" and should match most common case
 * @author Fabrice Bacchella
 *
 */
public class VarExtractor extends FieldsProcessor {

    private Pattern parser = Pattern.compile("(?<name>\\p{Alnum}+)\\p{Space}?[=:]\\p{Space}?(?<value>[^;,:]+)[;,:]?");
    ThreadLocal<Matcher> matchersGenerator = ThreadLocal.withInitial( () -> parser.matcher(""));

    @Override
    public Object fieldFunction(Event event, Object fieldValue) {
        boolean parsed = false;
        String message = fieldValue.toString();
        String after = message;
        Matcher m = matchersGenerator.get().reset(message);
        StringBuilder skipped = new StringBuilder(message.length());
        while(m.find()) {
            skipped.append(message.substring(m.regionStart(), m.start()));
            String key = m.group("name");
            String value = m.group("value");
            if (key != null && ! key.isEmpty()) {
                if (value != null) {
                    parsed = true;
                    event.put(key, value);
                }
            }
            after = message.substring(m.end());
            m.region(m.end(), m.regionEnd());
        }
        skipped.append(after);
        if (! parsed) {
            return FieldsProcessor.RUNSTATUS.FAILED;
        } else if (skipped.length() != 0) {
            return skipped.toString();
        } else {
            return FieldsProcessor.RUNSTATUS.REMOVE;
        }
    }

    @Override
    public String getName() {
        return "VarExtractor";
    }

    /**
     * @return the parser
     */
    public String getParser() {
        return parser.pattern();
    }

    /**
     * @param parser the parser to set
     */
    public void setParser(String parser) {
        this.parser = Pattern.compile(parser);
    }

}
