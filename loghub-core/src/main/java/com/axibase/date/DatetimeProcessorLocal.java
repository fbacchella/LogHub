package com.axibase.date;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

import static com.axibase.date.DatetimeProcessorUtil.toMillis;

class DatetimeProcessorLocal implements DatetimeProcessor {
    private final int fractionsOfSecond;
    private final ZoneOffsetType offsetType;
    private final ZoneId zoneId;

    DatetimeProcessorLocal(int fractionsOfSecond, ZoneOffsetType offsetType, ZoneId zoneId) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.offsetType = offsetType;
        this.zoneId = zoneId;
    }

    @Override
    public long parseMillis(String datetime) {
        return parseMillis(datetime, zoneId);
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return toMillis(DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, ' ', zoneId, offsetType));
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, ' ', zoneId, offsetType);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, ' ', zoneId, offsetType);
    }

    @Override
    public String print(long timestamp) {
        return DatetimeProcessorUtil.printIso8601(timestamp, ' ', zoneId, offsetType, fractionsOfSecond);
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        return DatetimeProcessorUtil.printIso8601(timestamp, ' ', zoneId, offsetType, fractionsOfSecond);
    }

    @Override
    public void appendTo(long timestamp, StringBuilder accumulator) {
        DatetimeProcessorUtil.printIso8601(timestamp, ' ', zoneId, offsetType, fractionsOfSecond, accumulator);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return DatetimeProcessorUtil.printIso8601(zonedDateTime.toLocalDateTime(), zonedDateTime.getOffset(), offsetType, ' ', fractionsOfSecond);
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorLocal(fractionsOfSecond, offsetType, zoneId);
    }

    @Override
    public boolean canParse(String date) {
        return DatetimeProcessorUtil.checkExpectedMilliseconds(date, fractionsOfSecond)
                && DatetimeProcessor.super.canParse(date);
    }
}
