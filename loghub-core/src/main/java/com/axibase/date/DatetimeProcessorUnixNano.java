package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

class DatetimeProcessorUnixNano implements NumericDateTimeProcessor {
    private final ZoneId zoneId;

    DatetimeProcessorUnixNano(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public long parseMillis(String datetime) {
        return getInstant(datetime).toEpochMilli();
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return getInstant(datetime).toEpochMilli();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return getInstant(datetime).atZone(zoneId);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return getInstant(datetime).atZone(zoneId);
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
    public String print(ZonedDateTime zonedDateTime) {
        return "" + zonedDateTime.toInstant().toEpochMilli();
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorUnixNano(zoneId);
    }

    private Instant getInstant(String datetime) {
        long value = Long.parseLong(datetime);
        return Instant.ofEpochSecond(0, 0).plusNanos(value);
    }
}
