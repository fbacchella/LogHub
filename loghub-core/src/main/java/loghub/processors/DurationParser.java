package loghub.processors;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
        } else if (value instanceof String s) {
            return parseDuration(s);
        } else {
            return RUNSTATUS.NOSTORE;
        }
    }

    private Object parseDuration(String input) {
        Matcher matcher = pattern.matcher(input);
        if (matcher.matches()) {
            long days = getGroupAsLong(matcher, "days");
            long hours = getGroupAsLong(matcher, "hours");
            long minutes = getGroupAsLong(matcher, "minutes");
            BigDecimal seconds = getGroupAsBigDecimal(matcher, "seconds");
            BigDecimal milliseconds = getGroupAsBigDecimal(matcher, "milliseconds");
            BigDecimal microseconds = getGroupAsBigDecimal(matcher, "microseconds");
            BigDecimal nanoseconds = getGroupAsBigDecimal(matcher, "nanoseconds");

            Duration duration = Duration.ofDays(days)
                                        .plusHours(hours)
                                        .plusMinutes(minutes);

            BigDecimal totalNanos = seconds.multiply(BigDecimal.valueOf(1_000_000_000))
                                           .add(milliseconds.multiply(BigDecimal.valueOf(1_000_000)))
                                           .add(microseconds.multiply(BigDecimal.valueOf(1_000)))
                                           .add(nanoseconds);

            BigDecimal[] secondsAndNanos = totalNanos.divideAndRemainder(BigDecimal.valueOf(1_000_000_000));
            long s = secondsAndNanos[0].longValue();
            long n = secondsAndNanos[1].setScale(0, RoundingMode.HALF_UP).longValue();

            return duration.plusSeconds(s).plusNanos(n);
        } else {
            return RUNSTATUS.FAILED;
        }
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

    private BigDecimal getGroupAsBigDecimal(Matcher matcher, String groupName) {
        try {
            String value = matcher.group(groupName);
            return value != null ? new BigDecimal(value) : BigDecimal.ZERO;
        } catch (IllegalArgumentException | IllegalStateException e) {
            // Group name doesn't exist in the pattern or not found, or not a valid number
            return BigDecimal.ZERO;
        }
    }

}
