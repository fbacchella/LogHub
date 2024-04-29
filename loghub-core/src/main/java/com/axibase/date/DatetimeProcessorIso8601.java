package com.axibase.date;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.function.BiConsumer;

import static com.axibase.date.DatetimeProcessorUtil.toMillis;

class DatetimeProcessorIso8601 implements DatetimeProcessor {
    private final int fractionsOfSecond;
    private final BiConsumer<StringBuilder, ZoneOffset> zoneOffsetType;
    private final ZoneId zoneId;
    private final char delimitor;

    DatetimeProcessorIso8601(int fractionsOfSecond, BiConsumer<StringBuilder, ZoneOffset> zoneOffsetType, ZoneId zoneId, char delimitor) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.zoneOffsetType = zoneOffsetType;
        this.zoneId = zoneId;
        this.delimitor = delimitor;
    }

    @Override
    public long parseMillis(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsOffsetDateTime(datetime, delimitor).toInstant().toEpochMilli();
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return toMillis(
                DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, delimitor, zoneId, zoneOffsetType)
        );
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, delimitor, zoneId, zoneOffsetType);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, delimitor, zoneId, zoneOffsetType);
    }

    @Override
    public String print(long timestamp) {
        return DatetimeProcessorUtil.printIso8601(timestamp, delimitor, zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        return DatetimeProcessorUtil.printIso8601(timestamp, delimitor, zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return DatetimeProcessorUtil.printIso8601(zonedDateTime.toLocalDateTime(), zonedDateTime.getOffset(), zoneOffsetType,
                delimitor, fractionsOfSecond);
    }

    @Override
    public void appendTo(long timestamp, StringBuilder accumulator) {
        DatetimeProcessorUtil.printIso8601(timestamp, delimitor, zoneId, zoneOffsetType, fractionsOfSecond, accumulator);
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this :
                new DatetimeProcessorIso8601(fractionsOfSecond, zoneOffsetType, zoneId, delimitor);
    }

    @Override
    public boolean canParse(String date) {
        return DatetimeProcessorUtil.checkExpectedMilliseconds(date, fractionsOfSecond)
                && DatetimeProcessor.super.canParse(date);
    }
}
