package com.axibase.date;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.zone.ZoneRules;
import java.util.Locale;

import static com.axibase.date.DatetimeProcessorUtil.checkOffset;
import static com.axibase.date.DatetimeProcessorUtil.extractOffset;
import static com.axibase.date.DatetimeProcessorUtil.parseInt;
import static com.axibase.date.DatetimeProcessorUtil.parseNanos;
import static com.axibase.date.DatetimeProcessorUtil.resolveDigitByCode;

class DatetimeProcessorIso8601 implements DatetimeProcessor {

    private static final int ISO_LENGTH = "1970-01-01T00:00:00.000000000+00:00:00".length();
    private static final Instant MOCK = Instant.now();


    static class ParsingContext {
        int offset;
    }

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
        return parseIso8601AsOffsetDateTime(datetime, delimitor).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parseIso8601AsZonedDateTime(datetime, delimitor, zoneId, zoneOffsetType);
    }

    @Override
    public String print(Instant timestamp) {
        return printIso8601(timestamp, delimitor, zoneId, zoneOffsetType, fractionsOfSecond);
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        return printIso8601(zonedDateTime.toLocalDateTime(), zonedDateTime.getOffset(), zoneOffsetType,
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

    private ZonedDateTime parseIso8601AsZonedDateTime(String date, char delimiter,
            ZoneId defaultOffset, AppendOffset offsetType) {
        try {
            ParsingContext context = new ParsingContext();
            LocalDateTime localDateTime = parseIso8601AsLocalDateTime(date, delimiter, context);
            ZoneId zoneId = extractOffset(date, context.offset, offsetType, defaultOffset);
            return ZonedDateTime.of(localDateTime, zoneId);
        } catch (DateTimeException e) {
            throw new DateTimeParseException("Failed to parse date " + date + ": " + e.getMessage(), date, 0, e);
        }
    }

    private LocalDateTime parseIso8601AsLocalDateTime(String date, char delimiter, ParsingContext context) {
        int length = date.length();
        int offset = context.offset;

        // extract year
        int year = parseInt(date, offset, offset += 4, length);
        checkOffset(date, offset, '-');

        // extract month
        int month = parseInt(date, offset += 1, offset += 2, length);
        checkOffset(date, offset, '-');

        // extract day
        int day = parseInt(date, offset += 1, offset += 2, length);
        checkOffset(date, offset, delimiter);

        // extract hours, minutes, seconds and milliseconds
        int hour = parseInt(date, offset += 1, offset += 2, length);
        checkOffset(date, offset, ':');

        int minutes = parseInt(date, offset += 1, offset += 2, length);

        // seconds can be optional
        int seconds;
        if (date.charAt(offset) == ':') {
            seconds = parseInt(date, offset += 1, offset += 2, length);
        } else {
            seconds = 0;
        }

        // milliseconds can be optional in the format
        int nanos;
        if (offset < length && date.charAt(offset) == '.') {
            int startPos = ++offset;
            int endPosExcl = Math.min(offset + 9, length);
            int frac = resolveDigitByCode(date, offset++);
            while (offset < endPosExcl) {
                int digit = date.charAt(offset) - '0';
                if (digit < 0 || digit > 9) {
                    break;
                }
                frac = frac * 10 + digit;
                ++offset;
            }
            nanos = parseNanos(frac, offset - startPos);
        } else {
            nanos = 0;
        }
        context.offset = offset;
        return LocalDateTime.of(year, month, day, hour, minutes, seconds, nanos);
    }

    private OffsetDateTime parseIso8601AsOffsetDateTime(String date, char delimiter) {
        try {
            ParsingContext parsingContext = new ParsingContext();
            LocalDateTime localDateTime = parseIso8601AsLocalDateTime(date, delimiter, parsingContext);
            ZoneOffset zoneOffset = DatetimeProcessorUtil.parseOffset(parsingContext.offset, date);
            return OffsetDateTime.of(localDateTime, zoneOffset);
        } catch (DateTimeException e) {
            throw new DateTimeParseException("Failed to parse date " + date + ": " + e.getMessage(), date, 0, e);
        }
    }

    /**
     * Optimized print of a timestamp in ISO8601 or local format: yyyy-MM-dd[T| ]HH:mm:ss[.SSS]Z
     * @param timestamp milliseconds since epoch
     * @param offsetType Zone offset format: ISO (+HH:mm), RFC (+HHmm), or NONE
     * @return String representation of the timestamp
     */
    private String printIso8601(Instant timestamp, char delimiter, ZoneId zone, AppendOffset offsetType, int fractionsOfSecond) {
        return printIso8601(timestamp, delimiter, zone, offsetType, fractionsOfSecond, new StringBuilder(ISO_LENGTH));
    }

    private String printIso8601(Instant timestamp, char delimiter, ZoneId zone, AppendOffset offsetType, int fractionsOfSecond, StringBuilder sb) {
        ZoneOffset offset;
        long secs;
        int nanos;
        if (zone instanceof ZoneOffset) {
            secs = timestamp.getEpochSecond();
            nanos = timestamp.getNano();
            offset = (ZoneOffset) zone;
        } else {
            ZoneRules rules = zone.getRules();
            if (rules.isFixedOffset()) {
                secs = timestamp.getEpochSecond();
                nanos = timestamp.getNano();
                offset = rules.getOffset(MOCK);
            } else {
                secs = timestamp.getEpochSecond();
                nanos = timestamp.getNano();
                offset = rules.getOffset(timestamp);
            }
        }
        LocalDateTime ldt = LocalDateTime.ofEpochSecond(secs, nanos, offset);
        return printIso8601(ldt, offset, offsetType, delimiter, fractionsOfSecond, sb);
    }

    /**
     * Optimized print of a timestamp in ISO8601 or local format: yyyy-MM-dd[T| ]HH:mm:ss[.SSS]
     * @param dateTime timestamp as LocalDateTime
     * @param offset time zone offset
     * @param offsetType Zone offset format: ISO (+HH:mm), RFC (+HHmm), or NONE
     * @return String representation of the timestamp
     */
    private String printIso8601(LocalDateTime dateTime, ZoneOffset offset, AppendOffset offsetType, char delimiter, int fractionsOfSecond) {
        return printIso8601(dateTime, offset, offsetType, delimiter, fractionsOfSecond, new StringBuilder(ISO_LENGTH));
    }

    private String printIso8601(LocalDateTime dateTime, ZoneOffset offset, AppendOffset offsetType, char delimiter, int fractionsOfSecond, StringBuilder sb) {
        DatetimeProcessorUtil.adjustPossiblyNegative(sb, dateTime.getYear(), 4).append('-');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getMonthValue(), 2).append('-');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getDayOfMonth(), 2).append(delimiter);
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getHour(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getMinute(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getSecond(), 2);
        if (fractionsOfSecond > 0 && dateTime.getNano() > 0) {
            sb.append('.');
            DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, dateTime.getNano() / DatetimeProcessorUtil.powerOfTen(9 - fractionsOfSecond), fractionsOfSecond);
            // Remove useless 0
            DatetimeProcessorUtil.cleanFormat(sb);
        }
        offsetType.append(sb, offset, dateTime.atZone(offset).toInstant());
        return sb.toString();
    }

}
