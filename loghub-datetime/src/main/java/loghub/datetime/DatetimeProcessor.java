package loghub.datetime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

public interface DatetimeProcessor {

    Instant parseInstant(String datetime);

    ZonedDateTime parse(String datetime);

    String print(Instant instant);

    String print(ZonedDateTime zonedDateTime);

    DatetimeProcessor withLocale(Locale locale);

    DatetimeProcessor withDefaultZone(ZoneId zoneId);

    static DatetimeProcessor of(String pattern) {
        return PatternResolver.createNewFormatter(pattern);
    }

}
