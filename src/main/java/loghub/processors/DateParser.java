package loghub.processors;

import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class DateParser extends FieldsProcessor {

    private final static Map<String,DateTimeFormatter> NAMEDPATTERNS = new HashMap<String,DateTimeFormatter>(){{
        put("RFC_1123_DATE_TIME", DateTimeFormatter.RFC_1123_DATE_TIME);
        put("ISO_INSTANT", DateTimeFormatter.ISO_INSTANT);
        put("BASIC_ISO_DATE", DateTimeFormatter.BASIC_ISO_DATE);
        put("ISO_LOCAL_DATE", DateTimeFormatter.ISO_LOCAL_DATE);
        put("ISO_OFFSET_DATE", DateTimeFormatter.ISO_OFFSET_DATE);
        put("ISO_DATE", DateTimeFormatter.ISO_DATE);
        put("ISO_LOCAL_TIME", DateTimeFormatter.ISO_LOCAL_TIME);
        put("ISO_OFFSET_TIME", DateTimeFormatter.ISO_OFFSET_TIME);
        put("ISO_TIME", DateTimeFormatter.ISO_TIME);
        put("ISO_LOCAL_DATE_TIME", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        put("ISO_OFFSET_DATE_TIME", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        put("ISO_ZONED_DATE_TIME", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        put("ISO_DATE_TIME", DateTimeFormatter.ISO_DATE_TIME);
        put("ISO_WEEK_DATE", DateTimeFormatter.ISO_WEEK_DATE);
        put("ISO_OFFSET_DATE", DateTimeFormatter.ISO_OFFSET_DATE);
    }};

    // A subset of temporal field that can be expect from some incomplete date time string
    private final static TemporalField[] someTemporalFields = new TemporalField[] {
            ChronoField.OFFSET_SECONDS,
            ChronoField.EPOCH_DAY, ChronoField.NANO_OF_DAY, ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_YEAR, ChronoField.YEAR,
            ChronoField.NANO_OF_SECOND, ChronoField.MICRO_OF_SECOND, ChronoField.INSTANT_SECONDS, ChronoField.MILLI_OF_SECOND,
    };
    private String[] patternsStrings;
    private DateTimeFormatter[] patterns = NAMEDPATTERNS.values().toArray(new DateTimeFormatter[] {});
    private Locale locale = Locale.ENGLISH;
    private ZoneId zone = ZoneId.systemDefault();

    @Override
    public boolean configure(Properties properties) {
        if(patternsStrings != null) {
            patterns = new DateTimeFormatter[patternsStrings.length];
            for(int i =  0 ; i < patternsStrings.length ; i++) {
                String patternString = patternsStrings[i];
                try {
                    patterns[i] = NAMEDPATTERNS.containsKey(patternString) ? NAMEDPATTERNS.get(patternString) : DateTimeFormatter.ofPattern(patternString, locale);
                } catch (IllegalArgumentException e) {
                    logger.error("invalid date time pattern: " + patternString);
                    return false;
                }
            }
        } else if (zone != null) {
            for(int i =  0 ; i < patterns.length ; i++) {
                patterns[i] = patterns[i].withZone(zone);
            }
        }
        return super.configure(properties);
    }

    /**
     * Try to extract the date from the pattern.
     * 
     * If the pattern is incomplete (is missing some field like year or day), it will extract from
     * current time
     * @see loghub.processors.FieldsProcessor#processMessage(loghub.Event, java.lang.String, java.lang.String)
     */
    @Override
    public boolean processMessage(Event event, String field, String destination)
            throws ProcessorException {
        String dateString = event.get(field).toString();
        logger.debug("trying to parse {} from {}", dateString, field);
        boolean converted = false;
        for(DateTimeFormatter format: patterns) {
            try {
                OffsetDateTime now;
                // Parse the string, but don't expect a full result
                TemporalAccessor ta = format.parse(dateString);

                // Try to resolve the time zone first
                ZoneId zi = ta.query(TemporalQueries.zone());
                ZoneOffset zo = ta.query(TemporalQueries.offset());
                if ( zo != null) {
                    now = OffsetDateTime.now(zo);
                } else if ( zi != null) {
                    now = OffsetDateTime.now(zi);
                } else {
                    now = OffsetDateTime.now(zone);
                }

                // We are rarely interested in sub second, drop it to don't have false value
                now = now.truncatedTo(ChronoUnit.SECONDS);

                // Ok now try some common fields, and it will resolve the real date of the event
                for (TemporalField cf: someTemporalFields) {
                    if (ta.isSupported(cf)) {
                        logger.trace("{} {}", cf, ta.getLong(cf));
                        now = now.with(cf, ta.getLong(cf));
                    }
                }
                // We should have a complete OffsetDateTime now
                logger.debug("Resolved to {}", now);
                Date date = Date.from(now.toInstant());
                if(Event.TIMESTAMPKEY.equals(destination)) {
                    event.setTimestamp(date);
                } else {
                    event.put(destination, date);
                }
                converted = true;
                break;
            } catch (DateTimeException e) {
                logger.debug("failed to parse date {}: {}", () -> dateString, () -> e.getMessage());
                //no problem, just wrong parser, keep going
            }
        }
        return converted;
    }

    @Override
    public String getName() {
        return null;
    }

    /**
     * @return the pattern
     */
    public String getPattern() {
        return patternsStrings[0];
    }

    /**
     * @param pattern the pattern to set
     */
    public void setPattern(String pattern) {
        this.patternsStrings = new String[] {pattern};
    }

    /**
     * @return the patterns
     */
    public String[] getPatterns() {
        return patternsStrings;
    }

    /**
     * @param patterns the patterns to set
     */
    public void setPatterns(String[] patterns) {
        this.patternsStrings = patterns;
    }

    public String getLocale() {
        return locale.toLanguageTag();
    }

    public void setLocale(String locale) {
        this.locale = Locale.forLanguageTag(locale);
    }

    public String getTimezone() {
        return zone.getId();
    }

    public void setTimezone(String zone) {
        this.zone = ZoneId.of(zone);
    }

}
