package com.axibase.date;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

import static com.axibase.date.DatetimeProcessorUtil.timestampToZonedDateTime;

class DatetimeProcessorUnixMillis implements DatetimeProcessor {
    private final ZoneId zoneId;

    DatetimeProcessorUnixMillis(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public long parseMillis(String datetime) {
        return Long.parseLong(datetime);
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return Long.parseLong(datetime);
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parse(datetime, zoneId);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return timestampToZonedDateTime(parseMillis(datetime), zoneId);
    }

    @Override
    public String print(long timestamp) {
        return "" + timestamp;
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        return print(timestamp);
    }

    @Override
    public void appendTo(long timestamp, StringBuilder accumulator) {
        accumulator.append(timestamp);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return "" + zonedDateTime.toInstant().toEpochMilli();
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorUnixMillis(zoneId);
    }
}
