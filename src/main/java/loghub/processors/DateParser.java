package loghub.processors;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.Level;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.OnMissingDateComponentAction;
import com.axibase.date.PatternResolver;

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class DateParser extends FieldsProcessor {

    private static final Map<String, DatetimeProcessor> NAMEDPATTERNS = new LinkedHashMap<String, DatetimeProcessor>();
    static {
        NAMEDPATTERNS.put("ISO_DATE_TIME", PatternResolver.createNewFormatter("iso_nanos"));
        NAMEDPATTERNS.put("ISO_INSTANT", PatternResolver.createNewFormatter("iso_nanos"));

        /*
                date-time   =  [ day "," ] date time       ; dd mm yy hh:mm:ss zzz
                day         =  "Mon"  / "Tue" /  "Wed"  / "Thu"
                               /  "Fri"  / "Sat" /  "Sun"
                date        =  1*2DIGIT month 2DIGIT       ; day month year
                                                           ;  e.g. 20 Jun 82
                month       =  "Jan"  /  "Feb" /  "Mar"  /  "Apr"
                            /  "May"  /  "Jun" /  "Jul"  /  "Aug"
                            /  "Sep"  /  "Oct" /  "Nov"  /  "Dec"
               time        =  hour zone                    ; ANSI and Military
               hour        =  2DIGIT ":" 2DIGIT [":" 2DIGIT]
                                                 ; 00:00:00 - 23:59:59
               zone        =  "UT"  / "GMT"                ; Universal Time
                                                           ; North American : UT
                           /  "EST" / "EDT"                ;  Eastern:  - 5/ - 4
                           /  "CST" / "CDT"                ;  Central:  - 6/ - 5
                           /  "MST" / "MDT"                ;  Mountain: - 7/ - 6
                           /  "PST" / "PDT"                ;  Pacific:  - 8/ - 7
                           /  1ALPHA                       ; Military: Z = UT;
                                                           ;  A:-1; (J not used)
                                                           ;  M:-12; N:+1; Y:+12
                           / ( ("+" / "-") 4DIGIT )        ; Local differential
                                                           ;  hours+min. (HHMM)
         */
        NAMEDPATTERNS.put("RFC_822_WEEK_DAY", PatternResolver.createNewFormatter("eee, d MMM yyyy HH:mm:ss Z"));
        NAMEDPATTERNS.put("RFC_822_SHORT", PatternResolver.createNewFormatter("d MMM yyyy HH:mm:ss Z"));
        // "Mmm dd hh:mm:ss"
        NAMEDPATTERNS.put("RFC_3164", PatternResolver.createNewFormatter("MMM d HH:mm:ss"));
        NAMEDPATTERNS.put("milliseconds", PatternResolver.createNewFormatter("milliseconds"));
        NAMEDPATTERNS.put("seconds", PatternResolver.createNewFormatter("seconds"));
    }

    private String[] patternsStrings;
    private DatetimeProcessor[] patterns = new DatetimeProcessor[0];
    private Locale locale = Locale.ENGLISH;
    private ZoneId zone = ZoneId.systemDefault();

    @Override
    public boolean configure(Properties properties) {
        if (patternsStrings != null) {
            // Keep null values, they are used to detect invalid patterns
            patterns = Arrays.stream(patternsStrings)
                             .map(this::resolveFromPattern)
                             .toArray(DatetimeProcessor[]::new);
        } else {
            patterns = NAMEDPATTERNS.values().toArray(new DatetimeProcessor[NAMEDPATTERNS.size()]);
        }
        return patterns.length != 0 && Arrays.stream(patterns).allMatch(Objects::nonNull) && super.configure(properties);
    }
    
    private DatetimeProcessor resolveFromPattern(String pattern) {
        DatetimeProcessor namedProcessor = NAMEDPATTERNS.get(pattern);
        if (namedProcessor != null) {
            return namedProcessor;
        }
        try {
            return PatternResolver.createNewFormatter(pattern, zone, OnMissingDateComponentAction.SET_CURRENT).withLocale(locale);
        } catch (IllegalArgumentException e) {
            logger.error("invalid date time pattern '{}' : {}", pattern, Helpers.resolveThrowableException(e));
            return null;
        }
    }

    /**
     * Resolve the format used when trying to parse the date. Since DatetimeProcessor implementation don't
     * override toString, using it for debugging is worthless.
     * Instead, if user-provided pattern is specified, find it by index.
     * Otherwise find the key in NAMEDPATTERNS map.
     * If debug is disabled, just return null.
     * @param index current patterns index
     * @param processor DatetimeProcessor used for parsing
     * @return date format
     */
    private String resolveCurrentlyUsedFormatIfDebug(int index, DatetimeProcessor processor) {
        if (!logger.isDebugEnabled()) {
            return null;
        }
        if (patternsStrings != null) {
            return patternsStrings[index];
        }
        for (Map.Entry<String, DatetimeProcessor> entry: NAMEDPATTERNS.entrySet()) {
            if (entry.getValue().equals(processor)) {
                return entry.getKey();
            }
        }
        return processor.toString();
    }

    /**
     * Try to extract the date from the pattern.
     * 
     * If the pattern is incomplete (is missing some field like year or day), it will extract from
     * current time
     * @see loghub.processors.FieldsProcessor#fieldFunction(loghub.Event, java.lang.Object)
     */
    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value instanceof Date || value instanceof TemporalAccessor) {
            return value;
        } else if (value instanceof Number) {
            // If parsing a number, only the first formatter is used
            return resolveFromNumber(event, patterns[0], (Number) value);
        } else {
            String dateString = value.toString();
            logger.debug("trying to parse {}", dateString);
            for (int i = 0; i < patterns.length; i++) {
                DatetimeProcessor formatter = patterns[i];
                String format = resolveCurrentlyUsedFormatIfDebug(i, formatter);
                logger.trace("trying to parse {} with {}", dateString, format);
                try {
                    ZonedDateTime dateParsed = formatter.parse(dateString);
                    logger.trace("parsed {} as {}", dateString, dateParsed);
                    return dateParsed.toInstant();
                } catch (IllegalArgumentException | DateTimeParseException e) {
                    //no problem, just wrong parser, keep trying
                    logger.debug("failed to parse date with pattern {}: {}", () -> format, () -> Helpers.resolveThrowableException(e));
                    logger.catching(Level.TRACE, e);
                }
            }
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
     }

    private Object resolveFromNumber(Event event, DatetimeProcessor formatter, Number value) throws ProcessorException {
        boolean isInstant = formatter == NAMEDPATTERNS.get("seconds");
        boolean isDate = formatter == NAMEDPATTERNS.get("milliseconds");
        Number n = (Number) value;
        if (n instanceof Float || n instanceof Double){
            double d = n.doubleValue();
            // The cast round toward 0
            long seconds = ((long)d);
            long nano = ((long)(d * 1e9) % 1_000_000_000l);
            return Instant.ofEpochSecond(seconds, nano);
        } else if (isInstant) {
            return Instant.ofEpochSecond(n.longValue(), 0);
        } else if (isDate) {
            return Instant.ofEpochMilli(n.longValue());
        } else {
            throw event.buildException("Don't know how to parse date value " + value);
        }
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
