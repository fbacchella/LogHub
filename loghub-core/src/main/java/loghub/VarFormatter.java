package loghub;

import java.io.UncheckedIOException;
import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.text.DateFormatSymbols;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
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
import java.util.Formatter;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private enum ARGUMENT_MODE {
        POSITIONAL,
        IMPLICIT,
        NAMED,
        STATIC
    }

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

    private record VariablePathHolder(String string, VariablePath path) {
        VariablePathHolder(String string) {
            this(string, VariablePath.parse(string));
        }
    }

    private abstract static class AbstractFormat implements Function<Object, CharSequence> {
        @Override
        public CharSequence apply(Object obj) {
            if (obj == null || obj == NullOrMissingValue.NULL) {
                return "null";
            } else if (obj == NullOrMissingValue.MISSING) {
                throw IgnoredEventException.INSTANCE;
            } else {
                return filteredFormat(obj);
            }
        }
        protected abstract CharSequence filteredFormat(Object obj);
    }

    private static final class JsonFormat extends AbstractFormat {
        private static final ObjectWriter writer;
        static {
            SimpleModule module = new SimpleModule("LogHub", new Version(1, 0, 0, null, "loghub", "EventToJson"));
            module.addSerializer(new EventSerializer());

            writer = JacksonBuilder.get(JsonMapper.class)
                                   .module(module)
                                   .setConfigurator(om -> om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false))
                                   .getWriter();
        }
        private static final JsonFormat INSTANCE = new JsonFormat();

        @Override
        public CharSequence filteredFormat(Object obj) {
            try {
                return writer.writeValueAsString(obj);
            } catch (JsonProcessingException e) {
                throw new UncheckedIOException("Can't serialized value: " + Helpers.resolveThrowableException(e), e);
            }
        }
    }

    private static final class FunctionFormat extends AbstractFormat {
        private final Locale l;
        private final boolean toUpper;
        private final Function<Object, CharSequence> f;

        private FunctionFormat(Locale l, boolean toUpper, Function<Object, CharSequence> f) {
            super();
            this.l = l;
            this.toUpper = toUpper;
            this.f = f;
        }

        @Override
        public CharSequence filteredFormat(Object obj) {
            if (toUpper) {
                return f.apply(obj).toString().toUpperCase(l);
            } else {
                return f.apply(obj);
            }
        }

    }

    private abstract static class JustifyFormat extends AbstractFormat {
        protected final ThreadLocal<DecimalFormat> f;
        protected final int size;
        protected final String padding;

        private JustifyFormat(Supplier<DecimalFormat> f, int size) {
            this.f = ThreadLocal.withInitial(f);
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
        public CharSequence filteredFormat(Object obj) {
            StringBuilder sb = new StringBuilder(size);
            sb.append(f.get().format(obj));
            if (size > sb.length()) {
                doPadding(sb, padding, size - sb.length());
            }
            return sb;
        }
        abstract void doPadding(StringBuilder toAppendTo, String padding, int newLength);
    }

    private static class RightJustifyNumberFormat extends JustifyFormat {
        RightJustifyNumberFormat(Supplier<DecimalFormat> f, int size) {
            super(f, size);
        }
        @Override
        void doPadding(StringBuilder toAppendTo, String padding, int newLength) {
            toAppendTo.insert(0, padding, 0, newLength);
        }
    }

    private static class LeftJustifyNumberFormat extends JustifyFormat {
        LeftJustifyNumberFormat(Supplier<DecimalFormat> f, int size) {
            super(f, size);
        }
        @Override
        void doPadding(StringBuilder toAppendTo, String padding, int newLength) {
            toAppendTo.append(padding, 0, newLength);
        }
    }

    private static final class NonDecimalNumberFormat extends AbstractFormat {
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
        public CharSequence filteredFormat(Object obj) {
            Number n = (Number) obj;
            // Short path for base 10 without padding
            if (base == 10 && ! flags.leftjustified && size == 0) {
                return Long.toString(n.longValue());
            }
            String formatted;
            String prefix;
            long longValue;
            switch (n) {
            case Byte b when base != 10 -> longValue = Byte.toUnsignedLong(b);
            case Short s when base != 10 -> longValue = Short.toUnsignedLong(s);
            case Integer i when base != 10 -> longValue = Integer.toUnsignedLong(i);
            default -> longValue = n.longValue();
            }
            boolean negative = longValue < 0;
            longValue = Math.abs(longValue);
            switch (base) {
            case 16 -> {
                formatted = Long.toHexString(longValue);
                prefix = toUpper ? "0X" : "0x";
            }
            case 8 -> {
                formatted = Long.toOctalString(longValue);
                prefix = "0";
            }
            default -> {
                formatted = Long.toString(longValue);
                prefix = "";
            }
            }
            // For eventual sign and prefix
            StringBuilder sb = new StringBuilder(Math.max(formatted.length(), size) + 2);
            if (!flags.zeropadded && size > 0 && !flags.leftjustified) {
                doPadding(sb, formatted, negative, ' ');
            }
            if (negative) {
                sb.append('-');
            }
            if (flags.alternateform) {
                sb.append(prefix);
            }
            if (toUpper) {
                formatted = formatted.toUpperCase(l);
            }
            if (formatted.length() < size && ! flags.leftjustified && flags.zeropadded) {
                doPadding(sb, formatted, negative, '0');
            }
            sb.append(formatted);
            if (formatted.length() < size && flags.leftjustified) {
                doPadding(sb, formatted, negative, ' ');
            }
            return sb;
        }
        private void doPadding(StringBuilder sb, String formatted, boolean negative, char padder) {
            int paddingsize = size - formatted.length() + (negative ? -1 : 0);
            if (paddingsize > 0) {
                char[] paddingprefix = new char[paddingsize];
                Arrays.fill(paddingprefix, padder);
                sb.append(paddingprefix);
            }
        }
    }

    static class SingleAppendable implements Appendable {
        StringBuilder sequence = new StringBuilder();
        @Override
        public Appendable append(CharSequence csq) {
            sequence.append(csq);
            return this;
        }
        @Override
        public Appendable append(CharSequence csq, int start, int end) {
            sequence.append(csq, start, end);
            return this;
        }
        @Override
        public Appendable append(char c) {
            sequence.append(c);
            return this;
        }
    }

    private static final class StandardFormat extends AbstractFormat {
        private final String format;
        private final Locale locale;

        private StandardFormat(Locale locale, String format) {
            this.format = format;
            this.locale = locale;
        }

        @Override
        protected CharSequence filteredFormat(Object obj) {
            SingleAppendable appendable = new SingleAppendable();
            try (Formatter f = new Formatter(appendable)) {
                f.format(locale, format, obj);
            }
            return appendable.sequence;
        }
    }

    private static final class ExtendedDateFormat extends AbstractFormat {
        private final ZoneId tz;
        private final ZoneId etz;
        private final boolean chronologyCheck;
        private final boolean isUpper;
        private final Locale locale;
        private final BiConsumer<StringBuilder, TemporalAccessor> taToStr;
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
                    ThreadLocal<DecimalFormat> tlDf = ThreadLocal.withInitial(() -> new DecimalFormat("0000", DecimalFormatSymbols.getInstance(locale)));
                    taToStr = (sb, ta) -> {
                        int offsetS = getTemporalAccessor(ta).get(ChronoField.OFFSET_SECONDS);
                        sb.append(offsetS < 0 ? '-' : '+');
                        int minutes = Math.abs(offsetS) / 60;
                        int offset = (minutes / 60) * 100 + (minutes % 60);
                        sb.append(tlDf.get().format(offset));
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
                    // Day of the year, formatted as three digits with leading zeros as necessary, e.g. 001 - 366 for the Gregorian calendar.
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
                    // Day of the month, formatted as two digits with leading zeros as necessary, i.e. 01 - 31
                    taToStr = formatTemporalAccessor("00", ChronoField.DAY_OF_MONTH);
                    zoned = true;
                    chronologyCheck = false;
                }
                case 'e' -> {
                    // Day of the month, formatted as two digits, i.e. 1 - 31.
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

        private BiConsumer<StringBuilder, TemporalAccessor> formatTemporalAccessor(String formatPattern, TemporalField field) {
            ThreadLocal<DecimalFormat> df = ThreadLocal.withInitial(() -> new DecimalFormat(formatPattern, DecimalFormatSymbols.getInstance(locale)));
            return (sb, ta) -> sb.append(df.get().format(ta.get(field)));
        }

        private BiConsumer<StringBuilder, TemporalAccessor> formatTemporalAccessor(String formatPattern, TemporalQuery<Long> transformd) {
            ThreadLocal<DecimalFormat> df = ThreadLocal.withInitial(() -> new DecimalFormat(formatPattern, DecimalFormatSymbols.getInstance(locale)));
            return (sb, ta) -> sb.append(df.get().format(transformd.queryFrom(ta)));
        }

        private TemporalAccessor withCalendarSystem(ZonedDateTime timePoint) {
            // Some Thai and Japanese locals need special treatment because of different calendar systems
            if (chronologyCheck) {
                return VarFormatter.resolveWithEra(locale, timePoint);
            } else {
                return timePoint;
            }
        }

        private TemporalAccessor getTemporalAccessor(Object obj) {
            return switch (obj) {
                case Instant i when zoned -> withCalendarSystem(i.atZone(etz));
                case ZonedDateTime zdt when tz != null -> zdt.withZoneSameInstant(tz);
                case LocalDateTime zdt -> zdt.atZone(etz);
                case TemporalAccessor ta -> ta;
                case Date d when zoned -> withCalendarSystem(d.toInstant().atZone(etz));
                case Date d -> d.toInstant();
                default -> throw new IllegalArgumentException("Not a date/time argument");
            };
        }

        @Override
        public CharSequence filteredFormat(Object obj) {
            if (! (obj instanceof Date) && ! (obj instanceof TemporalAccessor)) {
                return obj.toString();
            }
            try {
                StringBuilder sb = new StringBuilder();
                taToStr.accept(sb, getTemporalAccessor(obj));
                if (isUpper) {
                    return sb.toString().toUpperCase(locale);
                } else {
                    return sb;
                }
            } catch (DateTimeException e) {
                throw new IllegalArgumentException("Can't format the given time data: " + e.getMessage(), e);
            }
        }
    }

    private record BooleanFormat(boolean toUpper, Locale l) implements Function<Object, CharSequence> {
        @Override
            public CharSequence apply(Object obj) {
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
                return formatted;
            }
        }

    private static final Pattern varregexp = Pattern.compile("^(?<before>.*?(?=\\$\\{|\\{|'))\\$\\{(?<varname>[@#]?[\\w.-]+)?(?<format>%[^}]+)?}(?<after>.*)$", Pattern.DOTALL);
    private static final Pattern formatSpecifier = Pattern.compile("^(?<flag>[-#+ 0,(]*)?(?<length>\\d+)?(?:\\.(?<precision>\\d+))?(?:(?<istime>[tT])(?:<(?<tz>.*)>)?)?(?<conversion>[a-zA-Z%])(?::(?<locale>.+))?$", Pattern.DOTALL);
    private static final Pattern arrayIndex = Pattern.compile("#(?<index>\\d+)");

    private record FormatEntry(int index, Function<Object, CharSequence> formatter) {
        CharSequence format(Object[] resolved) {
            return formatter.apply(resolved[index]);
        }
    }

    private record FormatDelegated(Object[] parts) {
        // Buffer creation is done in two passes to avoid resizing: the first pass resolves all parts
        // and computes the total length, then the buffer is allocated at the exact capacity needed,
        // avoiding costly reallocations and improving performance.
        public String format(Object[] resolved) throws IllegalArgumentException {
            int size = 0;
            CharSequence[] elements = new CharSequence[parts.length];
            for (int i = 0; i < elements.length; i++) {
                Object o = parts[i];
                if (o instanceof CharSequence s) {
                    elements[i] = s;
                } else if (o instanceof FormatEntry fe) {
                    elements[i] = fe.format(resolved);
                }
                size += elements[i].length();
            }
            StringBuilder buffer = new StringBuilder(size);
            for (CharSequence cs: elements) {
                buffer.append(cs);
            }
            return buffer.toString();
        }

        @Override
        public String toString() {
            return "FormatDelegated{" + Arrays.toString(parts) + '}';
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FormatDelegated(Object[] parts1)))
                return false;

            return Arrays.equals(parts, parts1);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(parts);
        }
    }

    private final Object[] mapper;
    private final FormatDelegated delegated;
    private final ARGUMENT_MODE mode;

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
        List<Object> parts = new ArrayList<>();
        List<Object> constructMapper = new ArrayList<>();
        Set<ARGUMENT_MODE> varnames = new HashSet<>();
        // Convert the pattern to a MessageFormat which is compiled and be reused
        findVariables(format, constructMapper, parts, varnames);
        if (varnames.size() > 1) {
            throw new IllegalArgumentException("Can't mix indexed with object resolution");
        } else {
            mode = varnames.stream().findAny().orElse(ARGUMENT_MODE.STATIC);
        }
        mapper = constructMapper.toArray();
        delegated = new FormatDelegated(parts.toArray());
        empty = parts.size() == 1 && parts.getFirst() instanceof String;
    }

    public String argsFormat(Object... arg) throws IllegalArgumentException {
        if (arg.length == 1 && mode == ARGUMENT_MODE.IMPLICIT) {
            return format(arg[0]);
        } else {
            return format(arg);
        }
    }

    public String format(Object arg) throws IllegalArgumentException {
        Object[] resolved = new Object[mapper.length];
        if (! isEmpty()) {
            resolveArgs(arg, resolved);
        }
        return delegated.format(resolved);
    }

    @SuppressWarnings("unchecked")
    private void resolveArgs(Object arg, Object[] resolved) {
        Map<String, Object> variables;
        if (mode == ARGUMENT_MODE.STATIC) {
            return;
        } else if (mode == ARGUMENT_MODE.POSITIONAL && ! (arg instanceof List || arg.getClass().isArray())) {
            throw new IllegalArgumentException("Given a non-list to a format expecting only a list or an array");
        } else if (arg instanceof Map) {
            variables = (Map<String, Object>) arg;
        } else {
            variables = Map.of();
        }
        boolean withList = mode == ARGUMENT_MODE.POSITIONAL && arg instanceof List;
        boolean withArray = mode == ARGUMENT_MODE.POSITIONAL && arg.getClass().isArray();
        boolean withEvent = (mode == ARGUMENT_MODE.NAMED || mode == ARGUMENT_MODE.IMPLICIT) && arg instanceof Event;
        Event ev = withEvent ? (Event) arg : null;
        for (int i = 0; i < mapper.length ; i++) {
            resolved[i] = switch (mapper[i]) {
                case ARGUMENT_MODE.IMPLICIT -> checkArgType(arg);
                case VariablePathHolder vph when withEvent -> {
                    Object value = checkArgType(ev.getAtPath(vph.path));
                    if (value == NullOrMissingValue.MISSING) {
                        throw IgnoredEventException.INSTANCE;
                    } else {
                        yield value;
                    }
                }
                case Number n when withList -> {
                    int index = n.intValue();
                    List<Object> l = (List<Object>) arg;
                    if (index >= l.size()) {
                        throw new IllegalArgumentException("Not enough formatting arguments");
                    }
                    yield checkArgType(l.get(index));
                }
                case Number n when withArray -> {
                    int index = n.intValue();
                    if (index >= Array.getLength(arg)) {
                        throw new IllegalArgumentException("Not enough formatting arguments");
                    }
                    yield checkArgType(Array.get(arg, index));
                }
                case VariablePathHolder vph -> {
                    String key = vph.string;
                    String[] path = key.split("\\.");
                    if (path.length == 1) {
                        // Only one element in the key, just use it
                        if (!variables.containsKey(key)) {
                            throw new IllegalArgumentException("Invalid values for format key " + key);
                        } else {
                            yield checkArgType(variables.get(key));
                        }
                    } else {
                        // Recurse, variables written as "a.b.c" are paths in maps
                        Map<String, Object> current = variables;
                        key = path[0];
                        for (int j = 0; j < path.length - 1; j++) {
                            Map<String, Object> next = (Map<String, Object>) current.get(key);
                            if (next == null) {
                                throw new IllegalArgumentException("Invalid values for format key " + mapper[i]);
                            }
                            current = next;
                            key = path[i + 1];
                        }
                        yield checkArgType(current.get(key));
                    }
                }
                default -> throw new IllegalStateException("Unexpected value: " + mapper[i]);
            };
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

    private void findVariables(String in, List<Object> constructMapper, List<Object> parts, Set<ARGUMENT_MODE> varnames) {
        Matcher m = varregexp.matcher(in);
        if (m.find()) {
            String before = m.group("before");
            String varname = m.group("varname");
            String formatDefinition = m.group("format");
            String after = m.group("after");
            if (before != null && ! before.isEmpty()) {
                parts.add(before);
            }
            if (varname == null && formatDefinition == null) {
                // Not really a find, put back and continue
                parts.add("${}");
            } else {
                if (formatDefinition == null || formatDefinition.isEmpty()) {
                    formatDefinition = "%s";
                }
                if (varname == null || varname.isEmpty()) {
                    varname = ".";

                }
                Matcher listIndexMatch = arrayIndex.matcher(varname);
                if (listIndexMatch.matches()) {
                    int i = Integer.parseInt(listIndexMatch.group("index"));
                    constructMapper.add(i - 1);
                    varnames.add(ARGUMENT_MODE.POSITIONAL);
                } else if (".".equals(varname)) {
                    constructMapper.add(ARGUMENT_MODE.IMPLICIT);
                    varnames.add(ARGUMENT_MODE.IMPLICIT);
                } else {
                    constructMapper.add(new VariablePathHolder(varname));
                    varnames.add(ARGUMENT_MODE.NAMED);
                }
                parts.add(new FormatEntry(constructMapper.size() - 1, resolveFormat(formatDefinition.substring(1))));
            }
            findVariables(after, constructMapper, parts, varnames);
        } else {
            if (! in.isEmpty()) {
                parts.add(in);
            }
        }
    }

    private Function<Object, CharSequence> resolveFormat(String format) {
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

            return switch (conversion) {
                case 'b' -> new BooleanFormat(isUpper, locale);
                case 's' -> {
                    Function<Object, CharSequence> c;
                    if (precision < 0) {
                        c = Object::toString;
                    } else {
                        c = o -> o.toString().substring(0, precision);
                    }
                    yield new FunctionFormat(Locale.getDefault(), isUpper, c);
                }
                case 'h' -> {
                    // Can't use automatic upper case as it will transform the object
                    Function<Object, CharSequence> c;
                    if (isUpper) {
                        c = o -> Integer.toHexString(o.hashCode()).toUpperCase(Locale.getDefault());
                    } else {
                        c = o -> Integer.toHexString(o.hashCode());
                    }
                    yield new FunctionFormat(Locale.getDefault(), false, c);
                }
                case 'c' -> {
                    Function<Object, CharSequence> f = o -> {
                        char c;
                        if (o instanceof Character co) {
                            c = co;
                        } else {
                            c = o.toString().charAt(0);
                        }
                        return CharBuffer.wrap(new char[]{c});
                    };
                    yield new FunctionFormat(Locale.getDefault(), isUpper, f);
                }
                case 'd' -> {
                    if (checkCompatibleLocale() && !flags.parenthesis) {
                        yield new NonDecimalNumberFormat(locale, 10, false, flags, length);
                    }
                    yield numberFormat(locale, conversion, flags, length, isUpper);
                }
                case 'o', 'x' -> new NonDecimalNumberFormat(locale, conversion == 'o' ? 8 : 16, isUpper, flags, length);
                case 'e', 'f', 'g', 'a' -> new StandardFormat(locale, "%" + format);
                case 't' -> new ExtendedDateFormat(locale, timeFormat, ctz, isUpper);
                case '%' -> o -> "%";
                case 'n' -> {
                    String ls = System.lineSeparator();
                    yield o -> ls;
                }
                case 'j' -> JsonFormat.INSTANCE;
                default -> throw new IllegalArgumentException("Invalid format specifier: " + format);
            };
        } else {
            throw new IllegalArgumentException(format);
        }
    }

    private Function<Object, CharSequence> numberFormat(Locale l, char conversion, Flags flags, int length, boolean isUpper) {
        Supplier<DecimalFormat> dfSupplier = () -> {
            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(l);
            symbols.setExponentSeparator(isUpper ? "E" : "e");
            symbols.setDigit(flags.zeropadded ? '0' : '#');
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
            if (symbols.getDigit() == '0') {
                df.setMinimumIntegerDigits(length);
            }
            return df;
        };
        if (length < 0) {
            return new FunctionFormat(l, false, o -> {
                StringBuffer builder = new StringBuffer();
                dfSupplier.get().format(o, builder, new FieldPosition(0));
                return builder;
            });
        } else if (flags.leftjustified) {
            return new LeftJustifyNumberFormat(dfSupplier, length);
        } else {
            return new RightJustifyNumberFormat(dfSupplier, length);
        }
    }

    @Override
    public String toString() {
        return this.format;
    }

    public boolean checkCompatibleLocale() {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(locale);

        char zero = symbols.getZeroDigit();
        if ((zero != '0')) {
            return false;
        }
        DecimalFormat df = (DecimalFormat) NumberFormat.getNumberInstance(locale);
        if (! "-".equals(df.getNegativePrefix())) {
            return false;
        }
        if (! "".equals(df.getNegativeSuffix())) {
            return false;
        }
        if (! "".equals(df.getPositivePrefix())) {
            return false;
        }
        if (! "".equals(df.getPositiveSuffix())) {
            return false;
        }
        return true;
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
