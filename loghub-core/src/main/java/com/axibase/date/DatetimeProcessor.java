package com.axibase.date;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

public interface DatetimeProcessor extends DatetimePatternTester {
    long parseMillis(String datetime);

    long parseMillis(String datetime, ZoneId zoneId);

    ZonedDateTime parse(String datetime);

    ZonedDateTime parse(String datetime, ZoneId zoneId);

    String print(long timestamp);

    String print(long timestamp, ZoneId zoneId);

    String print(ZonedDateTime zonedDateTime);

    default void appendTo(long timestamp, StringBuilder accumulator) {
        accumulator.append(print(timestamp));
    }

    DatetimeProcessor withLocale(Locale locale);

    DatetimeProcessor withDefaultZone(ZoneId zoneId);

    default boolean canParse(String date) {
        try {
            final ZonedDateTime parsed = parse(date);
            final int year = parsed.getYear();
            return year >= DatetimeProcessorUtil.MIN_YEAR_20_CENTURY && year < DatetimeProcessorUtil.MAX_YEAR;
        } catch (Exception e) {
            return false;
        }
    }
}
