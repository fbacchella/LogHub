package loghub.processors;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.zone.ZoneRulesException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import loghub.datetime.DatetimeProcessor;
import loghub.datetime.NamedPatterns;

import loghub.BuilderClass;
import loghub.Expression;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Data;
import lombok.Setter;

@BuilderClass(DateParser.Builder.class)
public class DateParser extends FieldsProcessor {

    private static final String RFC_822_WEEK_DAY = "eee, d MMM yyyy HH:mm:ss Z";
    private static final String RFC_822_SHORT = "d MMM yyyy HH:mm:ss Z";
    private static final String RFC_3164 = "MMM d HH:mm:ss";

    private static final Map<String, DatetimeProcessor> NAMEDPATTERNS;
    static {
        DatetimeProcessor isoNanos = DatetimeProcessor.of(NamedPatterns.ISO_NANOS);
        DatetimeProcessor nanos = DatetimeProcessor.of(NamedPatterns.NANOSECONDS);
        DatetimeProcessor millis = DatetimeProcessor.of(NamedPatterns.MILLISECONDS);
        DatetimeProcessor seconds = DatetimeProcessor.of(NamedPatterns.SECONDS);
        NAMEDPATTERNS = Map.ofEntries(
            Map.entry("ISO", isoNanos),
            Map.entry("ISO_DATE_TIME", isoNanos),
            Map.entry("ISO_INSTANT", isoNanos),
            Map.entry("RFC_822_WEEK_DAY", DatetimeProcessor.of(RFC_822_WEEK_DAY)),
            Map.entry("RFC_822_SHORT", DatetimeProcessor.of(RFC_822_SHORT)),
            Map.entry("RFC_3164", DatetimeProcessor.of(RFC_3164)),
            Map.entry("NANOSECONDS", nanos),
            Map.entry("MILLISECONDS", millis),
            Map.entry("SECONDS", seconds),
            // For compatibility with logstash date processor
            Map.entry("ISO8601", isoNanos),
            Map.entry("UNIX", seconds),
            Map.entry("UNIX_MS", millis),
            Map.entry("UNIX_NS", nanos)
        );
    }

    @Data
    private static class DatetimeProcessorKey {
        private final String parser;
        private final String timezone;
        private final String locale;
        private DatetimeProcessor getDatetimeProcessor() {
            if (NAMEDPATTERNS.containsKey(parser.toUpperCase(Locale.ENGLISH))) {
                return NAMEDPATTERNS.get(parser.toUpperCase(Locale.ENGLISH)).withDefaultZone(ZoneId.of(timezone)).withLocale(Locale.forLanguageTag(locale));
            } else {
                return DatetimeProcessor.of(parser).withDefaultZone(ZoneId.of(timezone)).withLocale(Locale.forLanguageTag(locale));
            }
        }
    }

    private static final Map<DatetimeProcessorKey, DatetimeProcessor> processorsCache = new ConcurrentHashMap<>();

    @Setter
    public static class Builder extends FieldsProcessor.Builder<DateParser> {
        private Expression locale = new Expression(Locale.ENGLISH.getLanguage());
        private Expression timezone = new Expression(ZoneId.systemDefault());
        private String[] patterns = List.of(
            "iso",
            "RFC_822_WEEK_DAY",
            "RFC_822_SHORT",
            "RFC_3164",
            "milliseconds"
        ).toArray(String[]::new);
        public void setPattern(String pattern) {
            patterns = new String[]{pattern};
        }
        public DateParser build() {
            return new DateParser(this);
        }
    }
    public static DateParser.Builder getBuilder() {
        return new DateParser.Builder();
    }

    private final String[] patterns;
    private final Expression locale;
    private final Expression timezone;

    private DateParser(DateParser.Builder builder) {
        super(builder);
        patterns =  Arrays.copyOf(builder.patterns, builder.patterns.length);
        this.locale = builder.locale;
        this.timezone = builder.timezone;
        // Check parsers
        Arrays.stream(patterns).forEach(p -> new DatetimeProcessorKey(p, ZoneId.systemDefault().getId(), Locale.getDefault().toLanguageTag()).getDatetimeProcessor());
    }

    /**
     * Try to extract the date from the pattern.
     * If the pattern is incomplete (is missing some field like year or day), it will extract from
     * current time
     * @see loghub.processors.FieldsProcessor#fieldFunction(Event, java.lang.Object)
     */
    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value instanceof Date || value instanceof TemporalAccessor) {
            return value;
        } else if (value instanceof Number) {
            // If a number custom parsing
            return resolveFromNumber(event, (Number) value);
        } else {
            String dateString = value.toString();
            logger.debug("trying to parse {}", dateString);
            String eventTimeZone = timezone.eval(event).toString();
            String eventLocale = locale.eval(event).toString();
            for (String pattern : patterns) {
                DatetimeProcessorKey key = new DatetimeProcessorKey(pattern, eventTimeZone, eventLocale);
                try {
                    DatetimeProcessor formatter = processorsCache.computeIfAbsent(key, k -> key.getDatetimeProcessor());
                    logger.trace("trying to parse {} with pattern \"{}\"", dateString, key.parser);
                    ZonedDateTime dateParsed = formatter.parse(dateString);
                    logger.trace("parsed \"{}\" as {}", dateString, dateParsed);
                    return dateParsed.toInstant();
                } catch (ZoneRulesException ex) {
                    throw new ProcessorException(event, String.format(Helpers.resolveThrowableException(ex)));
                } catch (IllegalArgumentException | DateTimeParseException | StringIndexOutOfBoundsException ex) {
                    //no problem, just wrong parser, keep trying
                    logger.atDebug()
                          .withThrowable(logger.isTraceEnabled() ? ex : null)
                          .log("Failed to parse date with pattern \"{}\": {}",
                               () -> key.parser, ex::getMessage);
                }
            }
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
     }

    private Object resolveFromNumber(Event event, Number value) throws ProcessorException {
        boolean isInstant = NamedPatterns.SECONDS.equalsIgnoreCase(patterns[0]) || "UNIX".equalsIgnoreCase(patterns[0]);
        boolean isDate = NamedPatterns.MILLISECONDS.equalsIgnoreCase(patterns[0]) || "UNIX_MS".equalsIgnoreCase(patterns[0]);
        boolean isNano = "nanoseconds".equalsIgnoreCase(patterns[0]) || "UNIX_NS".equalsIgnoreCase(patterns[0]);
        if (value instanceof Float || value instanceof Double) {
            double d = value.doubleValue();
            // The cast round toward 0
            long seconds = ((long) d);
            long nano = ((long) (d * 1e9) % 1_000_000_000L);
            return Instant.ofEpochSecond(seconds, nano);
        } else if (isInstant) {
            return Instant.ofEpochSecond(value.longValue(), 0);
        } else if (isDate) {
            return Instant.ofEpochMilli(value.longValue());
        } else if (isNano) {
            return Instant.ofEpochSecond(0, 0).plusNanos(value.longValue());
        } else {
            throw event.buildException("Don't know how to parse date value " + value);
        }
    }

}
