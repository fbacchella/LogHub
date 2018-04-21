package loghub.processors;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class DateParser extends FieldsProcessor {

    private final static Map<String,DateTimeFormatter> NAMEDPATTERNS = new LinkedHashMap<String,DateTimeFormatter>();
    static {
        NAMEDPATTERNS.put("ISO_DATE_TIME", DateTimeFormatter.ISO_DATE_TIME);
        NAMEDPATTERNS.put("RFC_1123_DATE_TIME", DateTimeFormatter.RFC_1123_DATE_TIME);
        NAMEDPATTERNS.put("BASIC_ISO_DATE", DateTimeFormatter.BASIC_ISO_DATE);
        NAMEDPATTERNS.put("ISO_LOCAL_DATE", DateTimeFormatter.ISO_LOCAL_DATE);
        NAMEDPATTERNS.put("ISO_OFFSET_DATE", DateTimeFormatter.ISO_OFFSET_DATE);
        NAMEDPATTERNS.put("ISO_DATE", DateTimeFormatter.ISO_DATE);
        NAMEDPATTERNS.put("ISO_LOCAL_TIME", DateTimeFormatter.ISO_LOCAL_TIME);
        NAMEDPATTERNS.put("ISO_OFFSET_TIME", DateTimeFormatter.ISO_OFFSET_TIME);
        NAMEDPATTERNS.put("ISO_TIME", DateTimeFormatter.ISO_TIME);
        NAMEDPATTERNS.put("ISO_LOCAL_DATE_TIME", DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        NAMEDPATTERNS.put("ISO_OFFSET_DATE_TIME", DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        NAMEDPATTERNS.put("ISO_ZONED_DATE_TIME", DateTimeFormatter.ISO_ZONED_DATE_TIME);
        NAMEDPATTERNS.put("ISO_WEEK_DATE", DateTimeFormatter.ISO_WEEK_DATE);
        NAMEDPATTERNS.put("ISO_OFFSET_DATE", DateTimeFormatter.ISO_OFFSET_DATE);
        NAMEDPATTERNS.put("ISO_INSTANT", DateTimeFormatter.ISO_INSTANT);
    }

    // A subset of temporal field that can be expect from some incomplete date time string
    private final static TemporalField[] subSecondTemporalFields = new TemporalField[] {
            ChronoField.NANO_OF_SECOND, ChronoField.MICRO_OF_SECOND, ChronoField.MILLI_OF_SECOND
    };
    private final static TemporalField[] timeTemporalFields = new TemporalField[] {
            ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_DAY,
            ChronoField.SECOND_OF_DAY, ChronoField.MILLI_OF_DAY, ChronoField.MICRO_OF_DAY, ChronoField.NANO_OF_DAY, 
            ChronoField.HOUR_OF_AMPM, ChronoField.AMPM_OF_DAY, 
            ChronoField.SECOND_OF_MINUTE,
            ChronoField.MINUTE_OF_HOUR, 
    };
    private final static TemporalField[] dateTemporalFields = new TemporalField[] {
            ChronoField.EPOCH_DAY, ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_YEAR, ChronoField.YEAR, ChronoField.DAY_OF_MONTH, ChronoField.PROLEPTIC_MONTH, ChronoField.YEAR_OF_ERA
    };
    private final static String isoChronology = IsoChronology.INSTANCE.getId();

    private String[] patternsStrings;
    private DateTimeFormatter[] patterns = NAMEDPATTERNS.values().toArray(new DateTimeFormatter[NAMEDPATTERNS.size()]);
    private Locale locale = Locale.ENGLISH;
    private ZoneId zone = ZoneId.systemDefault();

    @Override
    public boolean configure(Properties properties) {
        if (patternsStrings != null) {
            patterns = Arrays.stream(patternsStrings)
                    .map(i -> {
                        try {
                            return NAMEDPATTERNS.containsKey(i) ? NAMEDPATTERNS.get(i) : DateTimeFormatter.ofPattern(i, locale);
                        } catch (IllegalArgumentException e) {
                            logger.error("invalid date time pattern '{}' : {}", i, e.getMessage());
                            return null;
                        }
                    })
                    .toArray(DateTimeFormatter[]::new);
            ;
        }
        return Arrays.stream(patterns).allMatch(i -> i != null) && super.configure(properties);
    }

    /**
     * Try to extract the date from the pattern.
     * 
     * If the pattern is incomplete (is missing some field like year or day), it will extract from
     * current time
     * @see loghub.processors.FieldsProcessor#processMessage(loghub.Event, java.lang.String, java.lang.String)
     */
    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        String dateString = event.get(field).toString();
        logger.debug("trying to parse {} from {}", dateString, field);
        boolean converted = false;
        for(DateTimeFormatter formatter: patterns) {
            logger.trace("trying to parse {} with {}", () -> dateString, () -> formatter.toString());
            try {
                TemporalAccessor ta = formatter.parse(dateString);
                logger.trace("parsed {} as {}", () -> dateString, () -> ta.toString());

                if (! isoChronology.equals(ta.query(TemporalQueries.chronology()).getId())) {
                    logger.warn("Can't hande non ISO chronology");
                    continue;
                }
                // Try to resolve the zone offset
                ZoneId zi = ta.query(TemporalQueries.offset());
                if (zi == null) {
                    zi = zone;
                }
                // Try to resolve the date
                LocalDate ld = ta.query(TemporalQueries.localDate());
                if (ld == null) {
                    ld = LocalDate.now(zi);
                    // date not found, but perhaps there is enough information anyway
                    // For example, when year is missing, so try to recover day and month
                    for (TemporalField cf: dateTemporalFields) {
                        if (ta.isSupported(cf)) {
                            logger.trace("{} {}", () -> cf, () -> ta.getLong(cf));
                            ld = ld.with(cf, ta.getLong(cf));
                        }
                    }
                }
                // Try to resolve the time
                LocalTime lt = ta.query(TemporalQueries.localTime());
                if (lt == null) {
                    lt = LocalTime.now(zi);
                    // We are rarely interested in sub second, drop it to don't have false value
                    lt = lt.truncatedTo(ChronoUnit.SECONDS);
                    // Ok now try to resolve sub-second precision
                    for (TemporalField cf: subSecondTemporalFields) {
                        if (ta.isSupported(cf)) {
                            logger.trace("{} {}", () -> cf, () -> ta.getLong(cf));
                            lt = lt.with(cf, ta.getLong(cf));
                            break;
                        }
                    }
                    for (TemporalField cf: timeTemporalFields) {
                        if (ta.isSupported(cf)) {
                            logger.trace("{} {}", () -> cf, () -> ta.getLong(cf));
                            lt = lt.with(cf, ta.getLong(cf));
                        }
                    }

                }
                ZoneOffset zo;
                if (zi instanceof ZoneOffset) {
                    zo = (ZoneOffset)zi;
                } else {
                    zo = zi.getRules().getOffset(LocalDateTime.of(ld, lt));
                }
                OffsetDateTime parsed = OffsetDateTime.of(ld, lt, zo);
                // We should have a complete OffsetDateTime now
                logger.debug("Resolved to {}", parsed);
                Date date = Date.from(parsed.toInstant());
                if(Event.TIMESTAMPKEY.equals(destination)) {
                    event.setTimestamp(date);
                } else {
                    event.put(destination, date);
                }
                converted = true;
                break;
            } catch (DateTimeException e) {
                logger.debug("failed to parse date with pattern {}: {}", () -> formatter.toString(), () -> e.getMessage());
                //no problem, just wrong parser, keep going
            }
        }
        return converted;
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
