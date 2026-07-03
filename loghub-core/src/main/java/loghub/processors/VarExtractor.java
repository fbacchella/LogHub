package loghub.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Setter;

/**
 * This transformer parses a field using a regex that extracts name and value.
 * The regex must contain two named group name and value. The field is parsed until exhaustion. The unmatched content will stay in the field
 * unless everything match; in this case, the field is removed.
 * <p>
 * The default parser is "(?&lt;name&gt;\p{Alnum}+)\p{Space}?[=:]\p{Space}?(?&lt;value&gt;[^;,:]+)[;,:]?" and should match most common case
 * @author Fabrice Bacchella
 *
 */
@BuilderClass(VarExtractor.Builder.class)
public class VarExtractor extends FieldsProcessor {

    public enum CollisionHandling {
        KEEP_FIRST,
        KEEP_LAST,
        AS_LIST,
    }

    @Setter
    public static class Builder extends FieldsProcessor.Builder<VarExtractor> {
        private Object parser = Pattern.compile("(?<name>\\p{Alnum}+)\\s?[=:]\\s?(?<value>[^;,:]+)[;,:]?");
        private CollisionHandling collision = CollisionHandling.KEEP_LAST;
        public VarExtractor build() {
            return new VarExtractor(this);
        }
    }
    public static VarExtractor.Builder getBuilder() {
        return new VarExtractor.Builder();
    }

    private static final Pattern NAMED_GROUP_PATTERN = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");

    private final ThreadLocal<Matcher> matchersGenerator;
    private final CollisionHandling collision;
    private final String[] valueGroups;

    public VarExtractor(Builder builder) {
        super(builder);
        String patternString = switch (builder.parser) {
            case Pattern p -> p.pattern();
            case null -> throw new IllegalArgumentException("Parser cannot be null");
            default -> builder.parser.toString();
        };

        List<String> allNamedGroups = NAMED_GROUP_PATTERN.matcher(patternString)
                                                         .results()
                                                         .map(mr -> mr.group(1))
                                                         .toList();

        long nameCount = allNamedGroups.stream().filter("name"::equals).count();
        long valueCount = allNamedGroups.stream().filter("value"::equals).count();

        if (nameCount == 0) {
            throw new IllegalArgumentException("Missing name named group in the pattern: " + patternString);
        } else if (nameCount > 1) {
            throw new IllegalArgumentException("Duplicate name named group in the pattern: " + patternString);
        }
        if (valueCount == 0) {
            throw new IllegalArgumentException("Missing value named group in the pattern: " + patternString);
        }

        Pattern parser;
        if (valueCount > 1) {
            StringBuilder newPatternString = new StringBuilder();
            int lastEnd = 0;
            int currentCount = 1;
            Matcher m = NAMED_GROUP_PATTERN.matcher(patternString);
            while (m.find()) {
                newPatternString.append(patternString, lastEnd, m.start());
                if ("value".equals(m.group(1))) {
                    newPatternString.append("(?<value").append(currentCount++).append(">");
                } else if ("name".equals(m.group(1))) {
                    newPatternString.append(m.group());
                } else {
                    throw new IllegalArgumentException("Unexpected named group in the pattern: " + m.group(1));
                }
                lastEnd = m.end();
            }
            newPatternString.append(patternString.substring(lastEnd));
            int flags = (builder.parser instanceof Pattern p) ? p.flags() : 0;
            parser = Pattern.compile(newPatternString.toString(), flags);
            valueGroups = java.util.stream.IntStream.rangeClosed(1, (int) valueCount)
                                                   .mapToObj(i -> "value" + i)
                                                   .toArray(String[]::new);
        } else {
            parser = (builder.parser instanceof Pattern p) ? p : Pattern.compile(patternString);
            valueGroups = new String[]{"value"};
        }

        matchersGenerator = ThreadLocal.withInitial(() -> parser.matcher(""));
        collision = builder.collision;
    }

    @Override
    public Object fieldFunction(Event event, Object fieldValue) {
        boolean parsed = false;
        String message = fieldValue.toString();
        Matcher m = matchersGenerator.get().reset(message);
        StringBuilder skipped = new StringBuilder(message.length());
        int lastEnd = 0;
        while (m.find()) {
            skipped.append(message, lastEnd, m.start());
            String key = m.group("name");
            if (key != null && ! key.isEmpty()) {
                for (String valueGroupName : valueGroups) {
                    String value = m.group(valueGroupName);
                    if (value != null) {
                        parsed = true;
                        if (! event.containsKey(key)) {
                            event.put(key, value);
                        } else {
                            switch (collision) {
                                case KEEP_LAST -> event.put(key, value);
                                case KEEP_FIRST -> {}
                                case AS_LIST -> event.merge(key, value, this::listDuplicate);
                            }
                        }
                    }
                }
            }
            lastEnd = m.end();
        }
        skipped.append(message.substring(lastEnd));
        if (! parsed) {
            return FieldsProcessor.RUNSTATUS.FAILED;
        } else if (!skipped.isEmpty()) {
            return skipped.toString();
        } else {
            return FieldsProcessor.RUNSTATUS.REMOVE;
        }
    }

    private <T> List<T> listDuplicate(T v1, T v2) {
        @SuppressWarnings("unchecked")
        List<T> l = (v1 instanceof List<?> list) ? (List<T>) list : new ArrayList<>(List.of(v1));
        l.add(v2);
        return l;
    }

    @Override
    public String getName() {
        return "VarExtractor";
    }

}
