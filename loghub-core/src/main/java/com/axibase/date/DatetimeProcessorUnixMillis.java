package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

class DatetimeProcessorUnixMillis implements NumericDateTimeProcessor {
    private final ZoneId zoneId;

    DatetimeProcessorUnixMillis(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public Instant parseInstant(String datetime) {
        return Instant.ofEpochMilli(Long.parseLong(datetime));
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parseInstant(datetime).atZone(zoneId);
    }

    @Override
    public String print(Instant timestamp) {
        return Long.toString(timestamp.toEpochMilli());
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return Long.toString(zonedDateTime.toInstant().toEpochMilli());
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
