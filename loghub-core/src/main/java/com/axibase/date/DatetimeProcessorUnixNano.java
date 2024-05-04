package com.axibase.date;

import java.math.BigInteger;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

class DatetimeProcessorUnixNano implements NumericDateTimeProcessor {

    private final ZoneId zoneId;
    private static final BigInteger ONE_MILLIARD = BigInteger.valueOf(1_000_000_000L);

    DatetimeProcessorUnixNano(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public Instant parseInstant(String datetime) {
        return getInstant(datetime);
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return getInstant(datetime).atZone(zoneId);
    }

    @Override
    public String print(Instant timestamp) {
        BigInteger nanos = BigInteger.valueOf(timestamp.getNano());
        return BigInteger.valueOf(timestamp.getEpochSecond()).multiply(ONE_MILLIARD).add(nanos).toString();
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
