package loghub;

import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.Format;
import java.text.MessageFormat;
import java.text.ParsePosition;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.JapaneseDate;
import java.time.chrono.ThaiBuddhistDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DecimalStyle;
import java.time.format.TextStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalQuery;
import java.time.temporal.WeekFields;
import java.time.zone.ZoneRules;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import loghub.events.Event;
import loghub.jackson.EventSerializer;
import loghub.jackson.JacksonBuilder;
import lombok.Getter;

public class VarFormatter {

    private record Flags(boolean leftjustified, boolean alternateform, boolean withsign, boolean leadingspace, boolean zeropadded, boolean grouping, boolean parenthesis) {
        private Flags(String flags) {
            this(flags.contains("-"),
                 flags.contains("#"),
                 flags.contains("+"),
                 flags.contains(" "),
                 flags.contains("0"),
                 flags.contains(","),
                 flags.contains("(")
            );
        }
    }

    private abstract static class NonParsingFormat extends Format {
        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse an object");
        }

        @Override
        public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            if (obj == NullOrMissingValue.NULL) {
                return toAppendTo.append("null");
            } else if (obj == NullOrMissingValue.MISSING) {
                throw IgnoredEventException.INSTANCE;
            } else {
                return filteredFormat(obj, toAppendTo, pos);
            }
        }
        protected StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            return toAppendTo;
        }
    }

    private static final class JsonFormat extends NonParsingFormat {
        private static final ObjectWriter writer;
        static {
            SimpleModule module = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "EventToJson"));
            module.addSerializer(new EventSerializer());

            writer = JacksonBuilder.get(JsonMapper.class)
                                   .module(module)
                                   .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                   .getWriter();
        }

        @Override
        public StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            try {
                return toAppendTo.append(writer.writeValueAsString(obj));
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException("Can't serialized value: " + Helpers.resolveThrowableException(e), e);
            }
        }
    }

    private static final class FunctionFormat extends NonParsingFormat {
        private final Locale l;
        private final boolean toUpper;
        private final Function<Object, String> f;

        private FunctionFormat(Locale l, boolean toUpper, Function<Object, String> f) {
            super();
            this.l = l;
            this.toUpper = toUpper;
            this.f = f;
        }

        @Override
        public StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo,
                                         FieldPosition pos) {
            String formatted = f.apply(obj);
            if (toUpper) {
                formatted = formatted.toUpperCase(l);
            }
            return toAppendTo.append(formatted);
        }

    }

    private static final class LitteralFormat extends NonParsingFormat {
        private final String litteral;
        private LitteralFormat(String litteral) {
            this.litteral = litteral;
        }

        @Override
        public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            return toAppendTo.append(litteral);
        }
    }

    private abstract static class JustifyFormat extends NonParsingFormat {
        protected final DecimalFormat f;
        protected final int size;
        protected final String padding;

        private JustifyFormat(DecimalFormat f, int size) {
            this.f = f;
            this.size = size;
            if (size > 0) {
                char[] paddingContent = new char[size];
                Arrays.fill(paddingContent, ' ');
                padding = new String(paddingContent);
            } else {
                padding = null;
            }
        }

        @Override
        public StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            toAppendTo.ensureCapacity(toAppendTo.length() + size);
            int oldLength = toAppendTo.length();
            f.format(obj, toAppendTo, pos);
            if ((oldLength + size) > toAppendTo.length()) {
                doPadding(toAppendTo, padding, oldLength, oldLength + size - toAppendTo.length());
            }
            return toAppendTo;
        }
        abstract void doPadding(StringBuffer toAppendTo, String padding, int oldLength, int newLenght);
    }

    private static class RightJustifyNumberFormat extends JustifyFormat {
        RightJustifyNumberFormat(DecimalFormat f, int size) {
            super(f, size);
        }
        @Override
        void doPadding(StringBuffer toAppendTo, String padding, int oldLength, int newLenght) {
            toAppendTo.insert(oldLength, padding, 0, newLenght);
        }
    }

    private static class LeftJustifyNumberFormat extends JustifyFormat {
        LeftJustifyNumberFormat(DecimalFormat f, int size) {
            super(f, size);
        }
        @Override
        void doPadding(StringBuffer toAppendTo, String padding, int oldLength, int newLength) {
            toAppendTo.append(padding, 0, newLength);
        }
    }

    private static final class NonDecimalNumberFormat extends NonParsingFormat {
        private final Locale l;
        private final int base;
        private final boolean toUpper;
        private final Flags flags;
        private final int size;
        private NonDecimalNumberFormat(Locale l, int base, boolean toUpper, Flags flags, int size) {
            this.l = l;
            this.base = base;
            this.toUpper = toUpper;
            this.flags = flags;
            this.size = size;
        }

        @Override
        public StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            Number n = (Number) obj;
            String formatted;
            String prefix;
            long longValue;
            if (n instanceof Byte && base != 10) {
                byte b = (byte) n;
                longValue = Byte.toUnsignedLong(b);
            } else if (n instanceof Short && base != 10) {
                short s = (short) n;
                longValue = Short.toUnsignedLong(s);
            } else if (n instanceof Integer && base != 10) {
                int i = (int) n;
                longValue = Integer.toUnsignedLong(i);
            } else {
                longValue = n.longValue();
            }
            switch (base) {
            case 16 -> {
                formatted = Long.toHexString(longValue);
                prefix = "0x";
            }
            case 8 -> {
                formatted = Long.toOctalString(n.longValue());
                prefix = "0";
            }
            default -> {
                formatted = Long.toString(n.longValue());
                prefix = "";
            }
            }
            if (flags.alternateform) {
                formatted = prefix + formatted;
            }
            if (toUpper) {
                formatted = formatted.toUpperCase(l);
            }
            if (formatted.length() < size && ! flags.leftjustified) {
                char[] paddingprefix = new char[size - formatted.length()];
                Arrays.fill(paddingprefix, '0');
                toAppendTo.append(paddingprefix);
            }

            return toAppendTo.append(formatted);
        }
    }

    private static final class ExtendedDateFormat extends NonParsingFormat {
        private final ZoneId tz;
        private final ZoneId etz;
        private final boolean chronologyCheck;
        private final boolean isUpper;
        private final Locale locale;
        private final BiConsumer<StringBuffer, TemporalAccessor> taToStr;
        private final boolean zoned;

        private ExtendedDateFormat(Locale l, char timeFormat, ZoneId tz, boolean isUpper) {
            this.tz = tz;
            this.etz = Optional.ofNullable(tz).orElse(ZoneId.systemDefault());
            this.locale = l;
            this.isUpper = isUpper;
            switch (timeFormat) {
                case 'H' -> {
                    // Hour of the day for the 24-hour clock, formatted as two digits with a leading zero as necessary i.e. 00 - 23.
                    taToStr = formatTemporalAccessor("00", ChronoField.HOUR_OF_DAY);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'I' -> {
                    // Hour for the 12-hour clock, formatted as two digits with a leading zero as necessary, i.e. 01 - 12.
                    taToStr = formatTemporalAccessor("00", ChronoField.CLOCK_HOUR_OF_AMPM);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'k' -> {
                    // Hour of the day for the 24-hour clock, i.e. 0 - 23.
                    taToStr = formatTemporalAccessor("#0", ChronoField.HOUR_OF_DAY);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'l' -> {
                    // Hour for the 12-hour clock, i.e. 1 - 12.
                    taToStr = formatTemporalAccessor("#0", ChronoField.CLOCK_HOUR_OF_AMPM);
                    // TZ goes to minute specifications
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'M' -> {
                    // Minute within the hour formatted as two digits with a leading zero as necessary, i.e. 00 - 59.
                    taToStr = formatTemporalAccessor("00", ChronoField.MINUTE_OF_HOUR);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'S' -> {
                    // Seconds within the minute, formatted as two digits with a leading zero as necessary, i.e. 00 - 60 ("60" is a special value required to support leap seconds).
                    taToStr = formatTemporalAccessor("00", ChronoField.SECOND_OF_MINUTE);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'L' -> {
                    // Millisecond within the second formatted as three digits with leading zeros as necessary, i.e. 000 - 999.
                    taToStr = formatTemporalAccessor("000", ChronoField.MILLI_OF_SECOND);
                    zoned = false;
                    chronologyCheck = false;
                }
                case 'N' -> {
                    // Nanosecond within the second, formatted as nine digits with leading zeros as necessary, i.e. 000000000 - 999999999.
                    taToStr = formatTemporalAccessor("000000000", ChronoField.NANO_OF_SECOND);
                    zoned = false;
                    chronologyCheck = false;
                }
                case 'p' -> {
                    // Locale-specific morning or afternoon marker in lower case, e.g."am" or "pm". Use of the conversion prefix 'T' forces this output to upper case.
                    String[] ampm = DateFormatSymbols.getInstance(l).getAmPmStrings();
                    taToStr = (sb, ta) -> {
                        String temp = ampm[ta.get(ChronoField.AMPM_OF_DAY)];
                        sb.append(isUpper ? temp.toUpperCase(l) : temp.toLowerCase(l));
                    };
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'z' -> {
                    // RFC 822 style numeric time zone offset from GMT, e.g. -0800. This value will be adjusted as necessary for Daylight Saving Time.
                    taToStr = (sb, ta) -> {
                        int offsetS = getTemporalAccessor(ta).get(ChronoField.OFFSET_SECONDS);
                        sb.append(offsetS < 0 ? '-' : '+');
                        int minutes = Math.abs(offsetS) / 60;
                        int offset = (minutes / 60) * 100 + (minutes % 60);
                        DecimalFormat df = new DecimalFormat("0000", DecimalFormatSymbols.getInstance(locale));
                        df.format(offset, sb, new FieldPosition(0));
                    };
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'Z' -> {
                    // A string representing the abbreviation for the time zone. This value will be adjusted as necessary for Daylight Saving Time.
                    ZoneRules zr = etz.getRules();
                    TimeZone timeZone = TimeZone.getTimeZone(etz);
                    taToStr = (sb, ta) -> sb.append(timeZone.getDisplayName(zr.isDaylightSavings(Instant.from(ta)), TimeZone.SHORT, l));
                    zoned = false;
                    chronologyCheck = false;
                }
                case 's' -> {
                    // Seconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC, i.e. Long.MIN_VALUE/1000 to Long.MAX_VALUE/1000.
                    taToStr = formatTemporalAccessor("#0", i -> Instant.from(i).getEpochSecond());
                    zoned = false;
                    chronologyCheck = false;
                }
                case 'Q' -> {
                    // Milliseconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC, i.e. Long.MIN_VALUE to Long.MAX_VALUE.
                    taToStr = formatTemporalAccessor("#0", i -> Instant.from(i).toEpochMilli());
                    zoned = false;
                    chronologyCheck = false;
                }
                case 'B' -> {
                    // Locale-specific full month name, e.g. "January", "February".
                    DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL).toFormatter(l);
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'b', 'h' -> {
                    // Locale-specific abbreviated month name, e.g. "Jan", "Feb", same as h.
                    // Locale-specific abbreviated month name, e.g. "Jan", "Feb", same as b.
                    DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT).toFormatter(l);
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'A' -> {
                    // Locale-specific full name of the day of the week, e.g. "Sunday", "Monday"
                    taToStr = (sb, ta) -> sb.append(DayOfWeek.from(ta).getDisplayName(TextStyle.FULL, l));
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'a' -> {
                    // Locale-specific short name of the day of the week, e.g. "Sun", "Mon"
                    taToStr = (sb, ta) -> sb.append(DayOfWeek.from(ta).getDisplayName(TextStyle.SHORT, l));
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'C' -> {
                    // Four-digit year divided by 100, formatted as two digits with leading zero as necessary, i.e. 00 - 99
                    taToStr = formatTemporalAccessor("00", i -> i.getLong(ChronoField.YEAR_OF_ERA) / 100);
                    zoned = true;
                    chronologyCheck = true;
                }
                case 'Y' -> {
                    // Year, formatted as at least four digits with leading zeros as necessary, e.g. 0092 equals 92 CE for the Gregorian calendar.
                    taToStr = formatTemporalAccessor("0000", ChronoField.YEAR_OF_ERA);
                    zoned = true;
                    chronologyCheck = true;
                }
                case 'y' -> {
                    // Last two digits of the year, formatted with leading zeros as necessary, i.e. 00 - 99.
                    taToStr = formatTemporalAccessor("00", i -> i.getLong(ChronoField.YEAR_OF_ERA) % 100);
                    zoned = true;
                    chronologyCheck = true;
                }
                case 'j' -> {
                    // Day of year, formatted as three digits with leading zeros as necessary, e.g. 001 - 366 for the Gregorian calendar.
                    taToStr = formatTemporalAccessor("000", ChronoField.DAY_OF_YEAR);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'm' -> {
                    // Month, formatted as two digits with leading zeros as necessary, i.e. 01 - 13.
                    taToStr = formatTemporalAccessor("00", ChronoField.MONTH_OF_YEAR);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'd' -> {
                    // Day of month, formatted as two digits with leading zeros as necessary, i.e. 01 - 31
                    taToStr = formatTemporalAccessor("00", ChronoField.DAY_OF_MONTH);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'e' -> {
                    // Day of month, formatted as two digits, i.e. 1 - 31.
                    taToStr = formatTemporalAccessor("#0", ChronoField.DAY_OF_MONTH);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'V' -> {
                    // The week number of the year (Monday as the first day of the week) as a decimal number (01-53).  If the week
                    // containing January 1 has four or more days in the new year, then it is week 1; otherwise it is the last week
                    // of the previous year, and the next week is week 1. Defined in ISO-8601
                    taToStr = formatTemporalAccessor("00", WeekFields.ISO.weekOfWeekBasedYear());
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'R' -> {
                    // Time formatted for the 24-hour clock as "%tH:%tM"
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'T' -> {
                    // Time formatted for the 24-hour clock as "%tH:%tM:%tS".
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'r' -> {
                    // Time formatted for the 12-hour clock as "%tI:%tM:%tS %Tp". The location of the morning or afternoon marker ('%Tp') may be locale-dependent.
                    DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("hh:mm:ss", locale).withDecimalStyle(DecimalStyle.of(l));
                    DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("a", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> {
                        dtf1.formatTo(ta, sb);
                        sb.append(' ');
                        String tzStr = dtf2.format(ta).toUpperCase();
                        sb.append(tzStr);
                    };
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'D' -> {
                    // Date formatted as "%tm/%td/%ty".
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yy", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = true;
                }
                case 'F' -> {
                    // ISO 8601 complete date formatted as "%tY-%tm-%td".
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = true;
                }
                case 'c' -> {
                    // Date and time formatted as "%ta %tb %td %tT %tZ %tY", e.g. "Sun Jul 20 16:17:00 EDT 1969".
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("eee MMM dd HH:mm:ss zz yyyy", locale).withDecimalStyle(DecimalStyle.of(l));
                    taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                    zoned = true;
                    chronologyCheck = true;
                }
                default -> throw new IllegalArgumentException("Unreconized date/time format: '" + timeFormat + "'");
            }
        }

        private BiConsumer<StringBuffer, TemporalAccessor> formatTemporalAccessor(String formatPattern, TemporalField field) {
            DecimalFormat df = new DecimalFormat(formatPattern, DecimalFormatSymbols.getInstance(locale));
            return (sb, ta) -> df.format(ta.get(field), sb, new FieldPosition(0));
        }

        private BiConsumer<StringBuffer, TemporalAccessor> formatTemporalAccessor(String formatPattern, TemporalQuery<Long> transformd) {
            DecimalFormat df = new DecimalFormat(formatPattern, DecimalFormatSymbols.getInstance(locale));
            return (sb, ta) -> df.format(transformd.queryFrom(ta), sb, new FieldPosition(0));
        }

        private TemporalAccessor withCalendarSystem(ZonedDateTime timePoint) {
            // Some thai and japanese locals needs special treatment because of different calendar systems
            if (chronologyCheck) {
                return VarFormatter.resolveWithEra(locale, timePoint);
            } else {
                return timePoint;
            }
        }

        private TemporalAccessor getTemporalAccessor(Object obj) {
            return switch (obj) {
                case Instant i when zoned -> withCalendarSystem(i.atZone(etz));
                case ZonedDateTime zdt when tz != null -> zdt.withZoneSameInstant(etz);
                case TemporalAccessor ta -> ta;
                default -> throw new IllegalArgumentException("Not a date/time argument");
            };
        }

        @Override
        public StringBuffer filteredFormat(Object obj, StringBuffer toAppendTo,
                                   FieldPosition pos) {
            if (! (obj instanceof Date) && ! (obj instanceof TemporalAccessor)) {
                return toAppendTo.append(obj);
            }
            try {
                if (isUpper) {
                    StringBuffer sb = new StringBuffer();
                    taToStr.accept(sb, getTemporalAccessor(obj));
                    toAppendTo.append(sb.toString().toUpperCase(locale));
                } else {
                    taToStr.accept(toAppendTo, getTemporalAccessor(obj));
                }
                return toAppendTo;
            } catch (DateTimeException e) {
                throw new IllegalArgumentException("Can't format the given time data: " + e.getMessage(), e);
            }
        }
    }

    private static final class BooleanFormat extends NonParsingFormat {
        private final boolean toUpper;
        private final Locale l;
        private BooleanFormat(boolean toUpper, Locale l) {
            this.toUpper = toUpper;
            this.l = l;
        }
        @Override
        public StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            String formatted;
            if (obj == null || obj == NullOrMissingValue.NULL) {
                formatted = "false";
            } else if (obj == NullOrMissingValue.MISSING) {
                throw IgnoredEventException.INSTANCE;
            } else if (obj instanceof Boolean b) {
                formatted = b.toString();
            } else {
                formatted = "true";
            }
            if (toUpper) {
                formatted = formatted.toUpperCase(l);
            }
            return toAppendTo.append(formatted);
        }
    }

    private static final Pattern varregexp = Pattern.compile("^(?<before>.*?(?=\\$\\{|\\{|'))(?:\\$\\{(?<varname>[@#]?[\\w.-]+)?(?<format>%[^}]+)?}|(?:(?<curlybraces>\\{.*})|(?<quote>')))(?<after>.*)$", Pattern.DOTALL);
    private static final Pattern formatSpecifier = Pattern.compile("^(?<flag>[-#+ 0,(]*)?(?<length>\\d+)?(?:\\.(?<precision>\\d+))?(?:(?<istime>[tT])(?:<(?<tz>.*)>)?)?(?<conversion>[a-zA-Z%])(?::(?<locale>.+))?$", Pattern.DOTALL);
    private static final Pattern arrayIndex = Pattern.compile("#(?<index>\\d+)");

    private static final Logger logger = LogManager.getLogger();

    private class FormatDelegated {
        private final MessageFormat mf;
        private final StringBuffer buffer = new StringBuffer();
        FormatDelegated(String pattern, List<String> formats) {
            try {
                mf = new MessageFormat(pattern, locale);
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException(String.format("Can't format %s, locale %s: %s", format, locale, ex.getMessage()), ex);
            }
            for (int i = 0; i < mf.getFormats().length; i++) {
                mf.setFormat(i, resolveFormat(formats.get(i)));
            }
        }
        public String format(Object[] resolved) throws IllegalArgumentException {
            buffer.setLength(0);
            try {
                return mf.format(resolved, buffer, new FieldPosition(0)).toString();
            } finally {
                buffer.setLength(0);
            }
        }
    }

    private final Map<Object, Integer> mapper;
    private final Function<Object[], String> delegated;

    private Locale locale;
    @Getter
    private final String format;
    @Getter
    private final boolean empty;

    public VarFormatter(String format) {
        this(format, Locale.getDefault());
    }

    /**
     * @param format a format string to be compiled
     * @param locale the {@link Locale} to use for formatting
     * @throws IllegalArgumentException if the format string can't be compiled
     */
    public VarFormatter(String format, Locale locale) {
        this.format = format.intern();
        this.locale = locale;
        List<String> formats = new ArrayList<>();
        Map<Object, Integer> constructMapper = new LinkedHashMap<>();
        // Convert the pattern to a MessageFormat which is compiled and be reused
        String pattern = findVariables(new StringBuilder(), format, 0, formats, constructMapper).toString();
        logger.trace("new format: {}, generated pattern {}", format, pattern);
        List<String> finalFormats = List.copyOf(formats);
        empty = constructMapper.isEmpty();
        if (! empty) {
            mapper = Map.copyOf(constructMapper);
            ThreadLocal<FormatDelegated> tl = ThreadLocal.withInitial(() -> new FormatDelegated(pattern, finalFormats));
            delegated = or -> tl.get().format(or);
            mapper.keySet().stream().reduce((i, j) ->  {
                if (i.getClass() != j.getClass()) {
                    throw new IllegalArgumentException("Can't mix indexed with object resolution");
                } else {
                    return j;
                }
            });
        } else {
            mapper = Map.of();
            delegated = or -> format;
        }
    }

    public String argsFormat(Object... arg) throws IllegalArgumentException {
        return format(arg);
    }

    public String format(Object arg) throws IllegalArgumentException {
        Object[] resolved = new Object[mapper.size()];
        if (! isEmpty()) {
            resolveArgs(arg, resolved);
        }
        return delegated.apply(resolved);
    }

    @SuppressWarnings("unchecked")
    private void resolveArgs(Object arg, Object[] resolved) {
        Map<String, Object> variables;
        Object mapperType = mapper.keySet().stream().findAny().orElse("");
        if ((mapperType instanceof Number) && ! (arg instanceof List || arg.getClass().isArray())) {
            throw new IllegalArgumentException("Given a non-list to a format expecting only a list or an array");
        } else if (arg instanceof Map) {
            variables = (Map<String, Object>) arg;
        } else {
            variables = Map.of();
        }
        for (Map.Entry<Object, Integer> mapping : mapper.entrySet()) {
            if (".".equals(mapping.getKey())) {
                resolved[mapping.getValue()] = checkArgType(arg);
            } else if (mapperType instanceof Number && arg instanceof List) {
                int i = ((Number) mapping.getKey()).intValue();
                int j = (mapping.getValue()).intValue();
                List<Object> l = (List<Object>) arg;
                if (j > l.size()) {
                    throw new IllegalArgumentException("index out of range");
                }
                resolved[i] = checkArgType(l.get(j - 1));
            } else if (mapperType instanceof Number && arg.getClass().isArray()) {
                int i = ((Number) mapping.getKey()).intValue();
                int j = (mapping.getValue()).intValue();
                if (j >  Array.getLength(arg)) {
                    throw new IllegalArgumentException("index out of range");
                }
                resolved[i] = checkArgType(Array.get(arg, j - 1));
            } else if (arg instanceof Event ev) {
                VariablePath vp = VariablePath.parse(mapping.getKey().toString());
                Object value = checkArgType(ev.getAtPath(vp));
                if (value == NullOrMissingValue.MISSING) {
                    throw IgnoredEventException.INSTANCE;
                }
                resolved[mapping.getValue()] = value;
            } else {
                String[] path = mapping.getKey().toString().split("\\.");
                if (path.length == 1) {
                    // Only one element in the key, just use it
                    if (! variables.containsKey(mapping.getKey())) {
                        throw new IllegalArgumentException("invalid values for format key " + mapping.getKey());
                    } else {
                        resolved[mapping.getValue()] = checkArgType(variables.get(mapping.getKey()));
                    }
                } else {
                    // Recurse, variables written as "a.b.c" are paths in maps
                    Map<String, Object> current = variables;
                    String key = path[0];
                    for (int i = 0; i < path.length - 1; i++) {
                        Map<String, Object> next = (Map<String, Object>) current.get(key);
                        if (next == null) {
                            throw new IllegalArgumentException("invalid values for format key " + mapping.getKey());
                        }
                        current = next;
                        key = path[i + 1];
                    }
                    resolved[mapping.getValue()] = checkArgType(current.get(key));
                }
            }
        }
    }

    private static final Locale LOCALEJAPANESERA = Locale.forLanguageTag("ja-JP-u-ca-japanese-x-lvariant-JP");
    private static final Locale LOCALETHAIERA = Locale.forLanguageTag("ja-JP-u-ca-japanese-x-lvariant-JP");

    private Object checkArgType(Object arg) {
        return switch (arg) {
            case null -> NullOrMissingValue.NULL;
            case Date d -> d.toInstant();
            case Calendar c ->
                    // Because Calendar type hierarchy is inconsistent and some class are not public
                    switch (c.getCalendarType()) {
                        case "gregory" -> ((GregorianCalendar) c).toZonedDateTime();
                        case "japanese" -> resolveWithEra(LOCALEJAPANESERA,
                                ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
                        case "buddhist" -> resolveWithEra(LOCALETHAIERA,
                                ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
                        default -> resolveWithEra(Locale.getDefault(),
                                ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
                    };
            case byte[] ba -> Arrays.toString(ba);
            case short[] sa -> Arrays.toString(sa);
            case int[] ia -> Arrays.toString(ia);
            case long[] la -> Arrays.toString(la);
            case float[] fa -> Arrays.toString(fa);
            case double[] da -> Arrays.toString(da);
            case boolean[] ba -> Arrays.toString(ba);
            case char[] ca -> Arrays.toString(ca);
            case Object[] oa -> Arrays.deepToString(oa);
            default -> arg;
        };
    }

    private StringBuilder findVariables(StringBuilder buffer, String in, int last, List<String> formats, Map<Object, Integer> constructMapper) {
        Matcher m = varregexp.matcher(in);
        if (m.find()) {
            String before = m.group("before");
            String varname = m.group("varname");
            String format = m.group("format");
            String curlybraces = m.group("curlybraces");
            String quote = m.group("quote");
            String after = m.group("after");
            buffer.append(before);
            if (curlybraces != null) {
                // Escape a {} pair
                buffer.append("'").append(curlybraces).append("'");
            } else if (quote != null) {
                // Escape a lone '
                buffer.append("''");
            } else if (varname == null && format == null) {
                // Not really a find, put back and continue
                buffer.append("$'{}'");
            } else {
                if (format == null || format.isEmpty()) {
                    format = "%s";
                }
                if (varname == null || varname.isEmpty()) {
                    varname = ".";
                }
                // Remove the initial %
                formats.add(format.substring(1));
                Matcher listIndexMatch = arrayIndex.matcher(varname);
                int index;
                if (listIndexMatch.matches()) {
                    index = last;
                    int i = Integer.parseInt(listIndexMatch.group("index"));
                    constructMapper.put(last++, i);
                } else if (! constructMapper.containsKey(varname)) {
                    index = last;
                    constructMapper.put(varname, last++);
                } else {
                    index = constructMapper.get(varname);
                }
                buffer.append("{").append(index).append("}");
            }
            findVariables(buffer, after, last, formats, constructMapper);
        } else {
            buffer.append(in);
        }
        return buffer;
    }

    private Format resolveFormat(String format) {
        Matcher m = formatSpecifier.matcher(format);
        if (m.matches()) {
            String localeStr = m.group("locale");
            if (localeStr != null && ! localeStr.isEmpty()) {
                locale = Locale.forLanguageTag(localeStr);
            }

            String lengthStr =  m.group("length");
            int length = lengthStr == null ? -1 : Integer.parseInt(lengthStr);
            String precisionStr =  m.group("precision");
            int precision = precisionStr == null ? -1 : Integer.parseInt(precisionStr);
            String flagStr = m.group("flag");
            if (flagStr == null) {
                flagStr = "";
            }
            Flags flags = new Flags(flagStr);
            String isTime =  m.group("istime");

            String conversionStr = m.group("conversion");

            char timeFormat = '\0';
            ZoneId ctz = null;
            if (isTime != null && ! isTime.isEmpty()) {
                timeFormat = conversionStr.charAt(0);
                conversionStr = isTime;
                String tzStr = m.group("tz");
                if (tzStr != null && ! tzStr.isEmpty()) {
                    ctz = ZoneId.of(tzStr);
                } else if (tzStr != null) {
                    ctz = ZoneId.systemDefault();
                }
            }

            boolean isUpper = conversionStr.toUpperCase(locale).equals(conversionStr);
            char conversion = conversionStr.toLowerCase(locale).charAt(0);

            final UnaryOperator<String> cut = i -> precision < 0 ? i : i.substring(0, precision);
            return switch (conversion) {
                case 'b' -> new BooleanFormat(isUpper, locale);
                case 's' -> new FunctionFormat(Locale.getDefault(), isUpper, o -> cut.apply(o.toString()));
                case 'h' -> new FunctionFormat(Locale.getDefault(), isUpper, o -> Integer.toHexString(o.hashCode()));
                case 'c' -> {
                    Function<Object, String> f = o -> (o instanceof Character) ? o.toString() : "null";
                    yield new FunctionFormat(Locale.getDefault(), isUpper, f);
                }
                case 'd' -> numberFormat(locale, conversion, flags, true, length, precision, isUpper);
                case 'o', 'x' -> new NonDecimalNumberFormat(locale, conversion == 'o' ? 8 : 16, isUpper, flags, length);
                case 'e', 'f', 'g', 'a' -> numberFormat(locale, conversion, flags, false, length, precision, isUpper);
                case 't' -> new ExtendedDateFormat(locale, timeFormat, ctz, isUpper);
                case '%' -> new LitteralFormat("%");
                case 'n' -> new LitteralFormat(System.lineSeparator());
                case 'j' -> new JsonFormat();
                default -> throw new IllegalArgumentException("Invalid format specifier: " + format);
            };
        } else {
            throw new IllegalArgumentException(format);
        }
    }

    private Format numberFormat(Locale l, char conversion, Flags flags, boolean integer, int length, int precision, boolean isUpper) {
        // Default precision for %f is exactly 6 digits
        precision = (precision == -1 ? 6 : precision);
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(l);
        symbols.setExponentSeparator(isUpper ? "E" : "e");
        symbols.setDigit(flags.zeropadded ? '0' : '#');
        int fixed = (integer ? length : length - precision - 1);
        DecimalFormat df = new DecimalFormat("#" + (conversion == 'e' ? "E00" : ""), symbols);
        if (flags.grouping) {
            df.setGroupingUsed(true);
            df.setGroupingSize(3);
        }
        if (flags.parenthesis) {
            df.setNegativePrefix("(");
            df.setNegativeSuffix(")");
        }
        if (flags.withsign) {
            df.setPositivePrefix("+");
        }
        if (! integer && precision >= 0) {
            df.setMinimumFractionDigits(precision);
            df.setMaximumFractionDigits(precision);
        }
        if (symbols.getDigit() == '0') {
            df.setMinimumIntegerDigits(fixed);
        }
        if (length < 0) {
            return new FunctionFormat(l, false, df::format);
        } else if (flags.leftjustified) {
            return new LeftJustifyNumberFormat(df, length);
        } else {
            return new RightJustifyNumberFormat(df, length);
        }
    }

    @Override
    public String toString() {
        return this.format;
    }

    static TemporalAccessor resolveWithEra(Locale locale, ZonedDateTime timePoint) {
        String language = locale.getLanguage();
        if ("th".equals(language) && "TH".equals(locale.getCountry())) {
            return ThaiBuddhistDate.from(timePoint).atTime(timePoint.toLocalTime()).atZone(timePoint.getZone());
        } else if ("ja".equals(language) && "japanese".equals(locale.getUnicodeLocaleType("ca"))) {
            return JapaneseDate.from(timePoint).atTime(timePoint.toLocalTime()).atZone(timePoint.getZone());
        } else {
            return timePoint;
        }
    }

}
