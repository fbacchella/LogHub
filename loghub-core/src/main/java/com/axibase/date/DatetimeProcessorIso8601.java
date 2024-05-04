package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

class DatetimeProcessorIso8601 implements DatetimeProcessor {
    private final int fractionsOfSecond;
    private final AppendOffset zoneOffsetType;
    private final ZoneId zoneId;
    private final char delimitor;

    DatetimeProcessorIso8601(int fractionsOfSecond, AppendOffset zoneOffsetType, ZoneId zoneId, char delimitor) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.zoneOffsetType = zoneOffsetType;
        this.zoneId = zoneId;
        this.delimitor = delimitor;
    }

    @Override
    public Instant parseInstant(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsOffsetDateTime(datetime, delimitor).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return DatetimeProcessorUtil.parseIso8601AsZonedDateTime(datetime, delimitor, zoneId, zoneOffsetType);
    }

    @Override
    public String print(Instant timestamp) {
        return DatetimeProcessorUtil.printIso8601(timestamp, delimitor, zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return DatetimeProcessorUtil.printIso8601(zonedDateTime.toLocalDateTime(), zonedDateTime.getOffset(), zoneOffsetType,
                delimitor, fractionsOfSecond);
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

}
