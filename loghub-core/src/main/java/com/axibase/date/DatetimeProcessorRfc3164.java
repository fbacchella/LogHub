package com.axibase.date;

import java.text.DateFormatSymbols;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.axibase.date.DatetimeProcessorUtil.adjustPossiblyNegative;
import static com.axibase.date.DatetimeProcessorUtil.appendNumberWithFixedPositions;
import static com.axibase.date.DatetimeProcessorUtil.checkOffset;
import static com.axibase.date.DatetimeProcessorUtil.parseInt;

public class DatetimeProcessorRfc3164 implements DatetimeProcessor {

    private final int dayLength;
    private final boolean withYear;
    private final int fractions;
    private final Locale locale;
    private final ZoneId zoneId;
    private final Map<String, Integer> monthsMapping;
    private final String[] shortMonths;
    private final AppendOffset zoneOffsetType;

    DatetimeProcessorRfc3164(int dayLength, boolean withYear, int fractions, AppendOffset zoneOffsetType) {
        this(dayLength, withYear, fractions, Locale.getDefault(), ZoneId.systemDefault(), zoneOffsetType);
    }

    DatetimeProcessorRfc3164(int dayLength, boolean withYear, int fractions, Locale locale, ZoneId zoneId, AppendOffset zoneOffsetType) {
        this.dayLength = dayLength;
        this.fractions = fractions;
        this.withYear = withYear;
        this.locale = locale;
        this.zoneId = zoneId;
        this.zoneOffsetType = zoneOffsetType;
        DateFormatSymbols symbols = new DateFormatSymbols(locale);
        this.shortMonths = symbols.getShortMonths();
        String[] monthsSymbols = symbols.getMonths();
        monthsMapping = IntStream.range(0, monthsSymbols.length).mapToObj(i -> Map.entry(monthsSymbols[i].toUpperCase(locale), i + 1))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Instant parseInstant(String datetime) {
        return parse(datetime).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        int length = datetime.length();
        int offset = 0;
        //Skip space
        while (offset < length && datetime.charAt(offset) == ' ') {offset++;}
        String monthName = datetime.substring(offset, offset+3).toUpperCase(locale);
        offset += 3;
        Integer month = monthsMapping.get(monthName);
        if (month == null) {
            throw new DateTimeParseException("Invalid month name", datetime, offset);
        }
        //Skip space
        while (datetime.charAt(offset) == ' ') {offset++;}
        int startDay = offset;
        while (Character.isDigit(datetime.charAt(offset))) {offset++;}
        int day = parseInt(datetime, startDay, offset, length);
        //Skip space
        while (datetime.charAt(offset) == ' ') {offset++;}
        int year;
        if (withYear) {
            year = parseInt(datetime, offset, offset += 4, length);
            while (datetime.charAt(offset) == ' ') {offset++;}
        } else {
            year = LocalDateTime.now().getYear();
        }
        int hour = parseInt(datetime, offset, offset += 2 , length);
        checkOffset(datetime, offset++, ':');
        int minutes = parseInt(datetime, offset, offset += 2, length);
        checkOffset(datetime, offset++, ':');
        int seconds = parseInt(datetime, offset, offset += 2 , length);
        DatetimeProcessorUtil.ParsingContext context = new DatetimeProcessorUtil.ParsingContext(offset);
        int nanos = DatetimeProcessorUtil.parseNano(length, context, datetime);
        offset = context.offset;
        //Skip space
        while (datetime.charAt(offset) == ' ') {offset++;}
        String zoneInfo = datetime.substring(offset);
        ZoneId parsedZoneId = this.zoneId;
        if (! zoneInfo.isEmpty()) {
            try {
                parsedZoneId = ZoneId.of(zoneInfo).normalized();
            } catch (DateTimeException ex) {
                throw new DateTimeParseException(ex.getMessage(), datetime, offset);
            }
        }
        return ZonedDateTime.of(year, month, day, hour, minutes, seconds, nanos, parsedZoneId);
    }

    @Override
    public String print(Instant timestamp) {
        return print(timestamp.atZone(zoneId));
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        StringBuilder formatted = new StringBuilder();
        formatted.append(shortMonths[zonedDateTime.getMonthValue() -1]);
        int day = zonedDateTime.getDayOfMonth();
        formatted.append(" ").append(day <= 9 ? "0": "").append(day);
        if (withYear) {
            formatted.append(" ");
            adjustPossiblyNegative(formatted, zonedDateTime.getYear(), 4);
        }
        formatted.append(" ");
        appendNumberWithFixedPositions(formatted, zonedDateTime.getHour(), 2).append(':');
        appendNumberWithFixedPositions(formatted, zonedDateTime.getMinute(), 2).append(':');
        appendNumberWithFixedPositions(formatted, zonedDateTime.getSecond(), 2);
        if (zoneOffsetType != null) {
            formatted.append(" ");
            zoneOffsetType.append(formatted, zonedDateTime.getOffset(), zonedDateTime.toInstant());
        }
        return formatted.toString();
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return new DatetimeProcessorRfc3164(this.dayLength, this.withYear, this.fractions, locale, this.zoneId, zoneOffsetType);
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return new DatetimeProcessorRfc3164(this.dayLength, this.withYear, this.fractions, this.locale, zoneId, zoneOffsetType);
    }

}
