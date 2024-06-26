package com.axibase.date;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Locale;

class DatetimeProcessorCustom implements DatetimeProcessor {
    private final DateTimeFormatter dateTimeFormatter;
    private final ZoneId zoneId;

    DatetimeProcessorCustom(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
        this.zoneId = ZoneId.systemDefault();
    }

    private DatetimeProcessorCustom(DateTimeFormatter dateTimeFormatter, ZoneId zid) {
        this.dateTimeFormatter = dateTimeFormatter;
        this.zoneId = zid;
    }

    @Override
    public Instant parseInstant(String datetime) {
        return parseMillisFailSafe(datetime).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parseMillisFailSafe(datetime);
    }

    @Override
    public String print(Instant timestamp) {
        return timestamp.atZone(zoneId).format(dateTimeFormatter);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return dateTimeFormatter.format(zonedDateTime);
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        if (locale.equals(dateTimeFormatter.getLocale())) {
            return this;
        }
        return new DatetimeProcessorCustom(dateTimeFormatter.withLocale(locale), zoneId);
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorCustom(dateTimeFormatter, zoneId);
    }

    private LocalDate resolveDateFromTemporal(TemporalAccessor parsed, ZoneId zone) {
        LocalDate query = parsed.query(TemporalQueries.localDate());
        if (query != null) {
            return query;
        }
        int year;
        int month;
        int day;
        LocalDate currentDate = null;
        if (parsed.isSupported(ChronoField.YEAR)) {
            year = parsed.get(ChronoField.YEAR);
        } else if (parsed.isSupported(ChronoField.YEAR_OF_ERA)) {
            year = parsed.get(ChronoField.YEAR_OF_ERA);
        } else if (parsed.isSupported(IsoFields.WEEK_BASED_YEAR)) {
            year = parsed.get(IsoFields.WEEK_BASED_YEAR);
        } else {
            currentDate = LocalDate.now(zone);
            year = currentDate.getYear();
        }
        if (parsed.isSupported(ChronoField.MONTH_OF_YEAR)) {
            month = parsed.get(ChronoField.MONTH_OF_YEAR);
        } else if (parsed.isSupported(IsoFields.QUARTER_OF_YEAR)) {
            int quarter = parsed.get(IsoFields.QUARTER_OF_YEAR);
            month = quarter * 3 - 2;
        } else {
            if (currentDate == null) {
                currentDate = LocalDate.now(zone);
            }
            month = currentDate.getMonthValue();
        }
        if (parsed.isSupported(ChronoField.DAY_OF_MONTH)) {
            day = parsed.get(ChronoField.DAY_OF_MONTH);
        } else {
            if (currentDate == null) {
                currentDate = LocalDate.now(zone);
            }
            day = currentDate.getDayOfMonth();
        }
        return LocalDate.of(year, month, day);
    }

    private static LocalTime resolveTimeFromTemporal(TemporalAccessor parsed) {
        LocalTime query = parsed.query(TemporalQueries.localTime());
        if (query != null) {
            return query;
        }
        int hour = 0;
        int minute = 0;
        int second = 0;
        int nanos = 0;
        if (parsed.isSupported(ChronoField.HOUR_OF_DAY)) {
            hour = parsed.get(ChronoField.HOUR_OF_DAY);
        }
        if (parsed.isSupported(ChronoField.MINUTE_OF_HOUR)) {
            minute = parsed.get(ChronoField.MINUTE_OF_HOUR);
        }
        if (parsed.isSupported(ChronoField.SECOND_OF_MINUTE)) {
            second = parsed.get(ChronoField.SECOND_OF_MINUTE);
        }
        if (parsed.isSupported(ChronoField.NANO_OF_SECOND)) {
            nanos = parsed.get(ChronoField.NANO_OF_SECOND);
        }
        return LocalTime.of(hour, minute, second, nanos);
    }

    /**
     * Parse datetime string  {@param value} using {@param formatter}. Provides default values for most fields,
     * so should be equivalent to SimpleDateFormat and joda-time for string-to-long timestamp converting operations.
     * @param datetime datetime as string
     * @return millis from epoch
     */
    private ZonedDateTime parseMillisFailSafe(String datetime) {
        TemporalAccessor parsed = dateTimeFormatter.parse(datetime);
        ZoneId zone = parsed.query(TemporalQueries.zone());
        if (zone == null) {
            zone = zoneId;
        }
        LocalDate localDate = resolveDateFromTemporal(parsed, zone);
        LocalTime localTime = resolveTimeFromTemporal(parsed);
        return ZonedDateTime.of(localDate, localTime, zone);
    }

}
