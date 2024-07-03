package loghub.datetime;

import java.text.DateFormatSymbols;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DatetimeProcessorRfc822 implements DatetimeProcessor {

    private final boolean weekDay;
    private final int dayLength;
    private final boolean withYear;
    private final int fractions;
    private final Locale locale;
    private final ZoneId zoneId;
    private final Map<String, Integer> monthsMapping;
    private final String[] shortWeekDays;
    private final String[] shortMonths;
    private final AppendOffset zoneOffsetType;
    private final ParseTimeZone tzParser;

    DatetimeProcessorRfc822(boolean weekDay, int dayLength, boolean withYear, int fractions, AppendOffset zoneOffsetType, ParseTimeZone tzParser) {
        this(weekDay, dayLength, withYear, fractions, Locale.getDefault(), ZoneId.systemDefault(), zoneOffsetType, tzParser);
    }

    private DatetimeProcessorRfc822(boolean weekDay, int dayLength, boolean withYear, int fractions, Locale locale, ZoneId zoneId, AppendOffset zoneOffsetType, ParseTimeZone tzParser) {
        this.weekDay = weekDay;
        this.fractions = fractions;
        this.dayLength = dayLength;
        this.withYear = withYear;
        this.locale = locale;
        this.zoneId = zoneId;
        this.zoneOffsetType = Optional.ofNullable(zoneOffsetType).map(z -> z.withLocale(locale)).orElse(null);
        this.tzParser = tzParser;
        DateFormatSymbols symbols = new DateFormatSymbols(locale);
        this.shortWeekDays = symbols.getShortWeekdays();
        this.shortMonths = symbols.getShortMonths();
        String[] monthsSymbols = symbols.getShortMonths();
        monthsMapping = IntStream.range(0, monthsSymbols.length)
                                 .mapToObj(i -> Map.entry(monthsSymbols[i].toUpperCase(locale), i + 1))
                                 .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Instant parseInstant(String datetime) {
        return parse(datetime).toInstant();
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        ParsingContext context = new ParsingContext(datetime);
        context.skipSpaces();
        // Skip day of week
        if (! (datetime.charAt(context.offset) >= '0' && datetime.charAt(context.offset) <= '9')) {
            while (datetime.charAt(context.offset) != ' ') {context.offset++;}
            if (datetime.charAt(context.offset) == ',') {
                context.offset++;
            }
        }
        context.skipSpaces();
        int day = context.parseInt(2);
        context.skipSpaces();
        String monthName = context.findWord().toUpperCase(locale);
        Integer month = monthsMapping.get(monthName);
        if (month == null) {
            throw context.parseException("Invalid month name");
        }
        context.skipSpaces();
        int year;
        if (withYear) {
            year = context.parseInt(-1);
            context.skipSpaces();
        } else {
            year = LocalDateTime.now().getYear();
        }
        int hour = context.parseInt(2);
        context.checkOffset(':');
        int minutes = context.parseInt(2);
        context.checkOffset(':');
        int seconds = context.parseInt(2);
        int nanos = context.parseNano();
        context.skipSpaces();
        ZoneId parsedZoneId = this.zoneId;
        String zoneInfo = context.findWord();
        if (! zoneInfo.isEmpty()) {
            try {
                parsedZoneId = ZoneId.of(zoneInfo).normalized();
            } catch (DateTimeException ex) {
                throw context.parseException(ex.getMessage());
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
        // First week day is the empty string
        String weekDay = shortWeekDays[zonedDateTime.get(ChronoField.DAY_OF_WEEK) + 1];
        formatted.append(weekDay).append(", ");
        int day = zonedDateTime.getDayOfMonth();
        formatted.append((day <= 9 && dayLength == 2) ? "0" : "").append(day);
        formatted.append(" ").append(shortMonths[zonedDateTime.getMonthValue() -1]);
        if (withYear) {
            formatted.append(" ");
            DatetimeProcessorUtil.adjustPossiblyNegative(formatted, zonedDateTime.getYear(), 4);
        }
        formatted.append(" ");
        DatetimeProcessorUtil.appendNumberWithFixedPositions(formatted, zonedDateTime.getHour(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(formatted, zonedDateTime.getMinute(), 2).append(':');
        DatetimeProcessorUtil.appendNumberWithFixedPositions(formatted, zonedDateTime.getSecond(), 2);
        DatetimeProcessorUtil.printSubSeconds(fractions, zonedDateTime::getNano, formatted);
        if (zoneOffsetType != null) {
            formatted.append(" ");
            zoneOffsetType.append(formatted, zonedDateTime);
        }
        return formatted.toString();
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return locale == this.locale ? this : new DatetimeProcessorRfc822(this.weekDay, this.dayLength, this.withYear, this.fractions, locale, this.zoneId, zoneOffsetType, tzParser);
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return zoneId == this.zoneId ? this : new DatetimeProcessorRfc822(this.weekDay, this.dayLength, this.withYear, this.fractions, this.locale, zoneId, zoneOffsetType, tzParser);
    }

}
