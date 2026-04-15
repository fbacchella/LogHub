package loghub.processors;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import loghub.BuilderClass;
import loghub.NullOrMissingValue;
import loghub.events.Event;
import lombok.Setter;

@BuilderClass(DurationParser.Builder.class)
public class DurationParser extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<DurationParser> {
        private Pattern pattern;
        public DurationParser build() {
            return new DurationParser(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private final Pattern pattern;

    private DurationParser(Builder builder) {
        super(builder);
        this.pattern = builder.pattern;
    }

    @Override
    public Object fieldFunction(Event event, Object value) {
        if (NullOrMissingValue.NULL.equals(value)) {
            return NullOrMissingValue.NULL;
        }
        String input = value.toString();
        Matcher matcher = pattern.matcher(input);
        if (matcher.matches()) {
            long days = getGroupAsLong(matcher, "days");
            long hours = getGroupAsLong(matcher, "hours");
            long minutes = getGroupAsLong(matcher, "minutes");
            double seconds = getGroupAsDouble(matcher, "seconds");
            long milliseconds = getGroupAsLong(matcher, "milliseconds");

            long fullSeconds = (long) Math.floor(seconds);
            long nanos = Math.round((seconds - fullSeconds) * 1_000_000_000) + milliseconds * 1_000_000;

            return Duration.ofDays(days)
                           .plusHours(hours)
                           .plusMinutes(minutes)
                           .plusSeconds(fullSeconds)
                           .plusNanos(nanos);
        }
        return RUNSTATUS.FAILED;
    }

    private long getGroupAsLong(Matcher matcher, String groupName) {
        try {
            String value = matcher.group(groupName);
            return value != null ? Long.parseLong(value) : 0L;
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Group name doesn't exist in the pattern or not found, or not a long (NumberFormatException)
            return 0L;
        }
    }

    private double getGroupAsDouble(Matcher matcher, String groupName) {
        try {
            String value = matcher.group(groupName);
            return value != null ? Double.parseDouble(value) : 0.0;
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Group name doesn't exist in the pattern or not found, or not a double (NumberFormatException)
            return 0.0;
        }
    }

}
