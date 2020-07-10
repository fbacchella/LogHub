package loghub.processors;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import com.axibase.date.DatetimeProcessor;
import com.axibase.date.PatternResolver;

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class DateParser extends FieldsProcessor {

    private final static Map<String, DatetimeProcessor> NAMEDPATTERNS = new LinkedHashMap<String, DatetimeProcessor>();
    static {
        NAMEDPATTERNS.put("ISO_DATE_TIME", PatternResolver.createNewFormatter("iso"));
        NAMEDPATTERNS.put("RFC_1123_DATE_TIME", PatternResolver.createNewFormatter("eee, d MMM yyyy HH:mm:ss Z"));
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
            patterns = Arrays.stream(patternsStrings)
                    .map(i -> {
                        try {
                            return (NAMEDPATTERNS.containsKey(i) ? NAMEDPATTERNS.get(i) : PatternResolver.createNewFormatter(i)).withLocale(locale).withDefaultZone(zone);
                        } catch (IllegalArgumentException e) {
                            logger.error("invalid date time pattern '{}' : {}", i, Helpers.resolveThrowableException(e));
                            return null;
                        }
                    })
                    .toArray(DatetimeProcessor[]::new);
            ;
        } else {
            patterns = NAMEDPATTERNS.values().stream().toArray(DatetimeProcessor[]::new);
        }
        return patterns.length != 0 && Arrays.stream(patterns).allMatch(i -> i != null) && super.configure(properties);
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
        String dateString = value.toString();
        logger.debug("trying to parse {}", dateString);
        for(DatetimeProcessor formatter: patterns) {
            logger.trace("trying to parse {} with {}", dateString, formatter);
            try {
                ZonedDateTime dateParsed = formatter.parse(dateString);
                logger.trace("parsed {} as {}", dateString, dateParsed);

                // Can't detect if date is missing, check for UNIX_EPOCH_YEAR and hope the best
                if (dateParsed.getYear() == 1970) {
                    ZonedDateTime now = ZonedDateTime.now();
                    dateParsed = dateParsed.withYear(now.getYear());
                    if (dateParsed.getMonthValue() == 1) {
                        dateParsed = dateParsed.withMonth(now.getMonthValue());
                        if (dateParsed.getDayOfMonth() == 1) {
                            dateParsed = dateParsed.withDayOfMonth(now.getDayOfMonth());
                        }
                    }
                }
                logger.debug("Resolved to {}", dateParsed);
                return dateParsed.toInstant();
            } catch (IllegalArgumentException e) {
                logger.debug("failed to parse date with pattern {}: {}", () -> formatter.toString(), () -> e.getMessage());
                //no problem, just wrong parser, keep going
            }
        }
        return FieldsProcessor.RUNSTATUS.FAILED;
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
