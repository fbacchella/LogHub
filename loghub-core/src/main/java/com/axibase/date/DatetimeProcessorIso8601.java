package com.axibase.date;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

import static com.axibase.date.DatetimeProcessorUtil.toMillis;

class DatetimeProcessorIso8601 implements DatetimeProcessor {
    private final int fractionsOfSecond;
    private final ZoneOffsetType zoneOffsetType;
    private final ZoneId zoneId;

    DatetimeProcessorIso8601(int fractionsOfSecond, ZoneOffsetType zoneOffsetType, ZoneId zoneId) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.zoneOffsetType = zoneOffsetType;
        this.zoneId = zoneId;
    }

    @Override
    public long parseMillis(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsOffsetDateTime(datetime, 'T').toInstant().toEpochMilli();
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return toMillis(
                DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, 'T', zoneId, zoneOffsetType)
        );
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, 'T', zoneId, zoneOffsetType);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, 'T', zoneId, zoneOffsetType);
    }

    @Override
    public String print(long timestamp) {
        return DatetimeProcessorUtil.printIso8601(timestamp, 'T', zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        return DatetimeProcessorUtil.printIso8601(timestamp, 'T', zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return DatetimeProcessorUtil.printIso8601(zonedDateTime.toLocalDateTime(), zonedDateTime.getOffset(), zoneOffsetType, 'T', fractionsOfSecond);
    }

    @Override
    public void appendTo(long timestamp, StringBuilder accumulator) {
        DatetimeProcessorUtil.printIso8601(timestamp, 'T', zoneId, zoneOffsetType, fractionsOfSecond, accumulator);
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this :
                new DatetimeProcessorIso8601(fractionsOfSecond, zoneOffsetType, zoneId);
    }

    @Override
    public boolean canParse(String date) {
        return DatetimeProcessorUtil.checkExpectedMilliseconds(date, fractionsOfSecond)
                && DatetimeProcessor.super.canParse(date);
    }
}
