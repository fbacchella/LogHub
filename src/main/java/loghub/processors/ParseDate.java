package loghub.processors;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class ParseDate extends FieldsProcessor {

    private static final Logger logger = LogManager.getLogger();

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

    private String[] patternsStrings;
    private DateTimeFormatter[] patterns = NAMEDPATTERNS.values().toArray(new DateTimeFormatter[] {});
    private Locale locale = Locale.ENGLISH;
    private ZoneId zone = null;

    @Override
    public boolean configure(Properties properties) {
        if(patternsStrings != null) {
            patterns = new DateTimeFormatter[patternsStrings.length];
            for(int i =  0 ; i < patternsStrings.length ; i++) {
                String patternString = patternsStrings[i];
                try {
                    patterns[i] = NAMEDPATTERNS.containsKey(patternString) ? NAMEDPATTERNS.get(patternString) : DateTimeFormatter.ofPattern(patternString, locale);
                    if(zone != null) {
                        patterns[i] = patterns[i].withZone(zone);
                    }
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

    @Override
    public void processMessage(Event event, String field, String destination)
            throws ProcessorException {
        String dateString = event.get(field).toString();
        boolean converted = false;
        for(DateTimeFormatter format: patterns) {
            try {
                TemporalAccessor ta = format.parse(dateString);
                Instant instant = Instant.from(ta);
                Date date = Date.from(instant);
                if("@timestamp".equals(destination)) {
                    event.timestamp = date;
                } else {
                    event.put(destination, date);
                }
                converted = true;
                break;
            } catch (DateTimeException e) {
                //no problem, just wrong parser, keep going
            }
        }
        if(!converted) {
            throw new ProcessorException("date string " + dateString + " not parsed", null);
        }

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
