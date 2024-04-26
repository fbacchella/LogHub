package com.axibase.date;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

import static com.axibase.date.DatetimeProcessorUtil.MAX_TIME_MILLIS;
import static com.axibase.date.DatetimeProcessorUtil.MILLISECONDS_IN_SECOND;

class DatetimeProcessorUnixSeconds implements DatetimeProcessor {
    private final ZoneId zoneId;

    DatetimeProcessorUnixSeconds(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    @Override
    public long parseMillis(String datetime) {
        if (datetime.indexOf('.') == -1) {
            return Long.parseLong(datetime) * MILLISECONDS_IN_SECOND;
        }
        return new BigDecimal(datetime).multiply(new BigDecimal(MILLISECONDS_IN_SECOND)).longValue();
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return parseMillis(datetime);
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parse(datetime, zoneId);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return DatetimeProcessorUtil.timestampToZonedDateTime(parseMillis(datetime), zoneId);
    }

    @Override
    public String print(long timestamp) {
        final long seconds = timestamp / MILLISECONDS_IN_SECOND;
        final long fractions = timestamp % MILLISECONDS_IN_SECOND;
        if (fractions == 0) {
            return Long.toString(seconds);
        }
        StringBuilder sb = new StringBuilder().append(seconds).append('.');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, (int)fractions, 3);
        return sb.toString();
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        return print(timestamp);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return print(zonedDateTime.toInstant().toEpochMilli());
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorUnixSeconds(zoneId);
    }

    @Override
    public boolean canParse(String date) {
        if (DatetimeProcessorUtil.isCreatable(date)) {
            final long millis = parseMillis(date, ZoneOffset.UTC);
            return millis >= 0 && millis < MAX_TIME_MILLIS;
        }
        return false;
    }
}
