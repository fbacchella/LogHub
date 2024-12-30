package loghub.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Setter;

/**
 * This transformer parse a field using a regex that extract name and value.
 * The regex must contain two named group name and value. The field is parsed until exhaustion. The unmatched content will stay in the field
 * unless everything match, in this case, the field is removed.
 * <p>
 * The default parser is "(?&lt;name&gt;\p{Alnum}+)\p{Space}?[=:]\p{Space}?(?&lt;value&gt;[^;,:]+)[;,:]?" and should match most common case
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(VarExtractor.Builder.class)
public class VarExtractor extends FieldsProcessor {

    public enum Collision_handling {
        KEEP_FIRST,
        KEEP_LAST,
        AS_LIST,
    }

    @Setter
    public static class Builder extends FieldsProcessor.Builder<VarExtractor> {
        private String parser = "(?<name>\\p{Alnum}+)\\s?[=:]\\s?(?<value>[^;,:]+)[;,:]?";
        private Collision_handling collision = Collision_handling.KEEP_LAST;
        public VarExtractor build() {
            return new VarExtractor(this);
        }
    }
    public static VarExtractor.Builder getBuilder() {
        return new VarExtractor.Builder();
    }

    private final ThreadLocal<Matcher> matchersGenerator;
    private final Collision_handling collision;

    public VarExtractor(Builder builder) {
        super(builder);
        Pattern parser = Pattern.compile(builder.parser);
        matchersGenerator = ThreadLocal.withInitial(() -> parser.matcher(""));
        collision = builder.collision;
    }

    @Override
    public Object fieldFunction(Event event, Object fieldValue) {
        boolean parsed = false;
        String message = fieldValue.toString();
        String after = message;
        Matcher m = matchersGenerator.get().reset(message);
        StringBuilder skipped = new StringBuilder(message.length());
        while (m.find()) {
            skipped.append(message, m.regionStart(), m.start());
            String key = m.group("name");
            String value = m.group("value");
            if (key != null && ! key.isEmpty() && value != null) {
                parsed = true;
                if (! event.containsKey(key)) {
                    event.put(key, value);
                } else {
                    switch (collision) {
                    case KEEP_LAST:
                        event.put(key, value);
                        break;
                    case KEEP_FIRST:
                        break;
                    case AS_LIST:
                        event.merge(key, value, this::listDuplicate);
                    }
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

    private <T> List<T> listDuplicate(T v1, T v2) {
        @SuppressWarnings("unchecked")
        List<T> l = (v1 instanceof List) ? (List<T>) v1 : new ArrayList<>(List.of(v1));
        l.add(v2);
        return l;
    }

    @Override
    public String getName() {
        return "VarExtractor";
    }

}
