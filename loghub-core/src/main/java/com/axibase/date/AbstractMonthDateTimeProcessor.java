package com.axibase.date;

import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;

abstract class AbstractMonthDateTimeProcessor implements DatetimeProcessor {
    final DateTimeFormatter formatter;
    final Map<String, Month> monthMap;
    final ZoneId defaultZone;

    AbstractMonthDateTimeProcessor(Locale locale, TextStyle textStyle, TextStyle standaloneTextStyle, ZoneId zoneId) {
        final DateTimeFormatter defaultFormatter = new DateTimeFormatterBuilder()
                .appendText(MONTH_OF_YEAR, textStyle)
                .toFormatter(locale);
        final DateTimeFormatter standaloneFormatter = new DateTimeFormatterBuilder()
                .appendText(MONTH_OF_YEAR, standaloneTextStyle)
                .toFormatter(locale);
        this.formatter = defaultFormatter;
        this.monthMap = prepareMap(defaultFormatter, standaloneFormatter);
        this.defaultZone = zoneId;
    }

    AbstractMonthDateTimeProcessor(DateTimeFormatter formatter, Map<String, Month> monthMap, ZoneId defaultZone) {
        this.formatter = formatter;
        this.monthMap = monthMap;
        this.defaultZone = defaultZone;
    }

    private static Map<String, Month> prepareMap(DateTimeFormatter formatter, DateTimeFormatter standaloneFormatter) {
        final Map<String, Month> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Month month : Month.values()) {
            addFormattedValue(formatter.format(month), month, result);
            addFormattedValue(standaloneFormatter.format(month), month, result);
        }
        return result;
    }

    private static void addFormattedValue(String value, Month month, Map<String, Month> map) {
        map.put(value, month);
        if (value.endsWith(".")) {
            map.put(value.substring(0, value.length() - 1), month);
        }
    }

    private Month parseMonth(String datetime) {
        return monthMap.get(datetime);
    }

    private ZonedDateTime monthToZonedDateTime(String month, ZoneId zoneId) {
        final Month parsedMonth = parseMonth(month);
        if (parsedMonth == null) {
            throw new DateTimeParseException("Text '" + month + "' could not be parsed at index 0", month, 0);
        }
        return ZonedDateTime.of(DatetimeProcessorUtil.UNIX_EPOCH_YEAR, parsedMonth.getValue(), 1, 0, 0, 0, 0, zoneId);
    }

    @Override
    public long parseMillis(String datetime) {
        return parseMillis(datetime, defaultZone);
    }

    @Override
    public long parseMillis(String datetime, ZoneId zoneId) {
        return DatetimeProcessorUtil.toMillis(parse(datetime, zoneId));
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parse(datetime, defaultZone);
    }

    @Override
    public ZonedDateTime parse(String datetime, ZoneId zoneId) {
        return monthToZonedDateTime(datetime, zoneId);
    }

    @Override
    public String print(long timestamp) {
        return print(timestamp, defaultZone);
    }

    @Override
    public String print(long timestamp, ZoneId zoneId) {
        final ZonedDateTime zonedDateTime = DatetimeProcessorUtil.timestampToZonedDateTime(timestamp, zoneId);
        return formatter.format(zonedDateTime);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return formatter.format(zonedDateTime);
    }

    @Override
    public void appendTo(long timestamp, StringBuilder accumulator) {
        final ZonedDateTime zonedDateTime = DatetimeProcessorUtil.timestampToZonedDateTime(timestamp, defaultZone);
        formatter.formatTo(zonedDateTime, accumulator);
    }

    @Override
    public boolean canParse(String date) {
        return parseMonth(date) != null && !DatetimeProcessorUtil.isNumeric(date);
    }
}
