package loghub.datetime;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;

class DatetimeProcessorIso8601 implements DatetimeProcessor {

    private static final int ISO_LENGTH = "1970-01-01T00:00:00.000000000+00:00:00".length();

    private final int fractionsOfSecond;
    private final AppendOffset zoneOffsetType;
    private final ParseTimeZone tzParser;
    private final ZoneId zoneId;
    private final char delimiter;

    DatetimeProcessorIso8601(int fractionsOfSecond, AppendOffset zoneOffsetType, ParseTimeZone tzParser, char delimiter) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.zoneOffsetType = zoneOffsetType;
        this.tzParser = tzParser;
        this.zoneId = ZoneId.systemDefault();
        this.delimiter = delimiter;
    }

    private DatetimeProcessorIso8601(int fractionsOfSecond, AppendOffset zoneOffsetType, ParseTimeZone tzParser, char delimiter, ZoneId zoneId) {
        this.fractionsOfSecond = fractionsOfSecond;
        this.zoneOffsetType = zoneOffsetType;
        this.tzParser = tzParser;
        this.zoneId = zoneId;
        this.delimiter = delimiter;
    }

    @Override
    public Instant parseInstant(String datetime) {
        return parse(datetime).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        ParsingContext context = new ParsingContext(datetime);
        // extract year
        int year = context.parseInt(-1);
        context.checkOffset('-');

        // extract month
        int month = context.parseInt(2);
        context.checkOffset('-');

        // extract day
        int day = context.parseInt(2);
        context.checkOffset(delimiter);

        // extract hours, minutes, seconds and milliseconds
        int hour = context.parseInt(2);
        context.checkOffset(':');

        int minutes = context.parseInt(2);

        // seconds can be optional
        int seconds;
        if (context.datetime.charAt(context.offset) == ':') {
            context.offset++;
            seconds = context.parseInt(2);
        } else {
            seconds = 0;
        }
        int nanos = context.parseNano();
        ZoneId parsedZoneId = context.extractOffset(zoneOffsetType, zoneId);
        return ZonedDateTime.of(year, month, day, hour, minutes, seconds, nanos, parsedZoneId);
    }

    @Override
    public String print(Instant timestamp) {
        return print(timestamp.atZone(zoneId));
    }

    @Override
    public String print(ZonedDateTime zonedDateTime) {
        StringBuilder sb = new StringBuilder(ISO_LENGTH);
        DatetimeProcessorUtil.adjustPossiblyNegative(sb, zonedDateTime.getYear(), 4).append('-');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, zonedDateTime.getMonthValue(), 2).append('-');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, zonedDateTime.getDayOfMonth(), 2).append(delimiter);
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, zonedDateTime.getHour(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, zonedDateTime.getMinute(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(sb, zonedDateTime.getSecond(), 2);
        DatetimeProcessorUtil.printSubSeconds(fractionsOfSecond, zonedDateTime::getNano, sb);
        zoneOffsetType.append(sb, zonedDateTime);
        return sb.toString();
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return this;
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this :
                new DatetimeProcessorIso8601(fractionsOfSecond, zoneOffsetType, tzParser, delimiter, zoneId);
    }

}
