package loghub.datetime;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Locale;

class DatetimeProcessorUnixSeconds implements NumericDateTimeProcessor {

    private static final BigDecimal ONE_MILLIARD = BigDecimal.valueOf(1_000_000_000L);

    private final ZoneId zoneId;
    private final Locale locale;
    private final String pattern;
    private final DecimalFormat formatter;

    DatetimeProcessorUnixSeconds() {
        this(ZoneId.systemDefault(), Locale.getDefault(), "#.###########");
    }

    private DatetimeProcessorUnixSeconds(ZoneId zoneId, Locale locale, String pattern) {
        this.zoneId = zoneId;
        this.locale = locale;
        this.pattern = pattern;
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(locale);
        formatter = new DecimalFormat(pattern, symbols);
    }

    @Override
    public Instant parseInstant(String datetime) {
        try {
            if (datetime.indexOf('.') == -1) {
                return Instant.ofEpochSecond(Long.parseLong(datetime));
            } else {
                BigDecimal floatValue = new BigDecimal(datetime);
                long seconds = floatValue.toBigInteger().longValue();
                int nano = floatValue.abs().subtract(new BigDecimal(seconds)).multiply(ONE_MILLIARD).toBigInteger().intValue();
                return Instant.ofEpochSecond(seconds, nano);
            }
        } catch (NumberFormatException e) {
            throw new DateTimeParseException("Not a number", datetime, 0);
        }
    }

    @Override
    public ZonedDateTime parse(String datetime) {
        return parseInstant(datetime).atZone(zoneId);
    }

    @Override
    public String print(Instant timestamp) {
        BigDecimal instantNumber = BigDecimal.valueOf(timestamp.getNano()).divide(ONE_MILLIARD, MathContext.UNLIMITED).add(BigDecimal.valueOf(timestamp.getEpochSecond()));
        return formatter.format(instantNumber, new StringBuffer(), new FieldPosition(0)).toString();
    }

    @Override
    public String print(ZonedDateTime timestamp) {
        return print(timestamp.toInstant());
    }

    @Override
    public DatetimeProcessor withLocale(Locale locale) {
        return locale == this.locale ? this : new DatetimeProcessorUnixSeconds(this.zoneId, locale, this.pattern);
    }

    @Override
    public DatetimeProcessor withDefaultZone(ZoneId zoneId) {
        return this.zoneId.equals(zoneId) ? this : new DatetimeProcessorUnixSeconds(zoneId, this.locale, this.pattern);
    }

}
