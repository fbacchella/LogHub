package com.axibase.date;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

public interface DatetimeProcessor {
    long parseMillis(String datetime);

    long parseMillis(String datetime, ZoneId zoneId);

    ZonedDateTime parse(String datetime);

    ZonedDateTime parse(String datetime, ZoneId zoneId);

    String print(long timestamp);

    String print(long timestamp, ZoneId zoneId);

    String print(ZonedDateTime zonedDateTime);

    DatetimeProcessor withLocale(Locale locale);

    DatetimeProcessor withDefaultZone(ZoneId zoneId);

}
