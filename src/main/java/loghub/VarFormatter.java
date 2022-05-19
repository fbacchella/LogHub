package loghub;

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
import java.time.chrono.ChronoLocalDate;
import java.time.chrono.ChronoZonedDateTime;
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
import java.util.Collections;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lombok.Getter;

public class VarFormatter {

    public static final DontCareFieldPosition INSTANCE = new DontCareFieldPosition(); 

    private static final class DontCareFieldPosition extends FieldPosition {
        private DontCareFieldPosition() {
            super(Integer.MIN_VALUE);
        }

        @Override
        public void setBeginIndex(int i) {
            // Do nothing
        }

        @Override
        public void setEndIndex(int i) {
            // Do nothing
        }

    }

    private static final class Flags {
        public final boolean leftjustified;
        public final boolean alternateform;
        public final boolean withsign;
        @SuppressWarnings("unused")
        public final boolean leadingspace;
        public final boolean zeropadded;
        public final boolean grouping;
        public final boolean parenthesis;

        private Flags(String flags) {
            boolean leftjustified = false;
            boolean alternateform = false;
            boolean withsign = false;
            boolean leadingspace = false;
            boolean zeropadded = false;
            boolean grouping = false;
            boolean parenthesis = false;
            for(char c: flags.toCharArray()) {
                switch(c) {
                case '-': leftjustified = true; break;
                case '#': alternateform = true; break;
                case '+': withsign = true; break;
                case ' ': leadingspace = true; break;
                case '0': zeropadded = true; break;
                case ',': grouping = true; break;
                case '(': parenthesis = true; break;
                default: throw new IllegalStateException("Unhandled flag format, should not be reached");
                }
            }
            this.leftjustified = leftjustified;
            this.alternateform = alternateform;
            this.withsign = withsign;
            this.leadingspace = leadingspace;
            this.zeropadded = zeropadded;
            this.grouping = grouping;
            this.parenthesis = parenthesis;
        }
    }

    private static final class NonParsingFormat extends Format {
        private final Locale l;
        private final boolean toUpper;
        private final Function<Object, String > f;

        private NonParsingFormat(Locale l, boolean toUpper, Function<Object, String > f) {
            super();
            this.l = l;
            this.toUpper = toUpper;
            this.f = f;
        }

        @Override
        public final StringBuffer format(Object obj, StringBuffer toAppendTo,
                                         FieldPosition pos) {
            String formatted = f.apply(obj);
            if (toUpper) {
                formatted = formatted.toUpperCase(l);
            }
            return toAppendTo.append(formatted);
        }
        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse an object");
        }
    }

    private static final class RightJustifyFormat extends Format {
        private final Format f;
        private final int size;
        private RightJustifyFormat(Format f, int size) {
            this.f = f;
            this.size = size;
        }
        public final StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            String formatted = f.format(obj, new StringBuffer(), pos).toString();
            if(formatted.length() < size) {
                char[] prefix = new char[size - formatted.length()];
                Arrays.fill(prefix, ' ');
                toAppendTo.append(prefix);
            }
            return toAppendTo.append(formatted);
        }
        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse an object");
        }
    }

    private static final class LeftJustifyFormat extends Format {
        private final Format f;
        private final int size;
        private LeftJustifyFormat(Format f, int size) {
            this.f = f;
            this.size = size;
        }
        public final StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            String formatted = f.format(obj, new StringBuffer(), pos).toString();
            toAppendTo.append(formatted);
            if (formatted.length() < size) {
                char[] prefix = new char[size - formatted.length()];
                Arrays.fill(prefix, ' ');
                toAppendTo.append(prefix);
            }
            return toAppendTo;
        }
        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse an object");
        }
    }

    private static final class NonDecimalFormat extends Format {
        private final Locale l;
        private final int base;
        private final boolean toUpper;
        private final Flags flags;
        private final int size;
        private NonDecimalFormat(Locale l, int base, boolean toUpper, Flags flags, int size) {
            this.l = l;
            this.base = base;
            this.toUpper = toUpper;
            this.flags = flags;
            this.size = size;
        }
        public final StringBuffer format(Object obj, StringBuffer toAppendTo, FieldPosition pos) {
            Number n = (Number) obj;
            String formatted;
            String prefix;
            if (base == 16) {
                formatted = Long.toHexString(n.longValue());
                prefix = "0x";
            } else if (base == 8) {
                formatted = Long.toOctalString(n.longValue());
                prefix = "0";
            } else {
                formatted = Long.toString(n.longValue());
                prefix = "";
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
        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse an object");
        }
    }

    private static final class ExtendedDateFormat extends Format {
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
            case 'H':
                // Hour of the day for the 24-hour clock, formatted as two digits with a leading zero as necessary i.e. 00 - 23.
                taToStr = formatTemporalAccessor(l, "00", ChronoField.HOUR_OF_DAY);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'I':
                // Hour for the 12-hour clock, formatted as two digits with a leading zero as necessary, i.e. 01 - 12.
                taToStr = formatTemporalAccessor(l, "00", ChronoField.CLOCK_HOUR_OF_AMPM);
                zoned = true;
                chronologyCheck = false;
               break;
            case 'k':
                // Hour of the day for the 24-hour clock, i.e. 0 - 23.
                taToStr = formatTemporalAccessor(l, "#0", ChronoField.HOUR_OF_DAY);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'l':
                // Hour for the 12-hour clock, i.e. 1 - 12.
                taToStr = formatTemporalAccessor(l, "#0", ChronoField.CLOCK_HOUR_OF_AMPM);
                // TZ goes to minute specifications
                zoned = true;
                chronologyCheck = false;
                break;
            case 'M':
                // Minute within the hour formatted as two digits with a leading zero as necessary, i.e. 00 - 59.
                taToStr = formatTemporalAccessor(l, "00", ChronoField.MINUTE_OF_HOUR);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'S':
                // Seconds within the minute, formatted as two digits with a leading zero as necessary, i.e. 00 - 60 ("60" is a special value required to support leap seconds).
                taToStr = formatTemporalAccessor(l, "00", ChronoField.SECOND_OF_MINUTE);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'L':
                // Millisecond within the second formatted as three digits with leading zeros as necessary, i.e. 000 - 999.
                taToStr = formatTemporalAccessor(l, "000", ChronoField.MILLI_OF_SECOND);
                zoned = false;
                chronologyCheck = false;
                break;
            case 'N':
                // Nanosecond within the second, formatted as nine digits with leading zeros as necessary, i.e. 000000000 - 999999999.
                taToStr = formatTemporalAccessor(l, "000000000", ChronoField.NANO_OF_SECOND);
                zoned = false;
                chronologyCheck = false;
                break;
            case 'p': {
                // Locale-specific morning or afternoon marker in lower case, e.g."am" or "pm". Use of the conversion prefix 'T' forces this output to upper case.
                String[] ampm = DateFormatSymbols.getInstance(l).getAmPmStrings();
                taToStr = (sb, ta) -> {
                    String temp = ampm[ta.get(ChronoField.AMPM_OF_DAY)];
                    sb.append(isUpper ? temp.toUpperCase(l) : temp.toLowerCase(l));
                };
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'z': {
                // RFC 822 style numeric time zone offset from GMT, e.g. -0800. This value will be adjusted as necessary for Daylight Saving Time.
                taToStr = (sb, ta) -> {
                    int offsetS = getTemporalAccessor(ta).get(ChronoField.OFFSET_SECONDS);
                    sb.append(offsetS < 0 ? '-' : '+');
                    int minutes = Math.abs(offsetS) / 60;
                    int offset = (minutes / 60) * 100 + (minutes % 60);
                    DecimalFormat df = getDecimalFormat("0000", l).get();
                    df.format(offset, sb, INSTANCE);
                };
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'Z': {
                // A string representing the abbreviation for the time zone. This value will be adjusted as necessary for Daylight Saving Time.
                ZoneRules zr = etz.getRules();
                TimeZone timeZone = TimeZone.getTimeZone(etz);
                taToStr = (sb, ta) -> sb.append(timeZone.getDisplayName(zr.isDaylightSavings(Instant.from(ta)), TimeZone.SHORT, l));
                zoned = false;
                chronologyCheck = false;
                break;
            }
            case 's':
                // Seconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC, i.e. Long.MIN_VALUE/1000 to Long.MAX_VALUE/1000.
                taToStr = formatTemporalAccessor(l, "#0", i -> Instant.from(i).getEpochSecond());
                zoned = false;
                chronologyCheck = false;
               break;
            case 'Q':
                // Milliseconds since the beginning of the epoch starting at 1 January 1970 00:00:00 UTC, i.e. Long.MIN_VALUE to Long.MAX_VALUE.
                taToStr = formatTemporalAccessor(l, "#0", i -> Instant.from(i).toEpochMilli());
                zoned = false;
                chronologyCheck = false;
                break;
            case 'B': {
                // Locale-specific full month name, e.g. "January", "February".
                DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendText(ChronoField.MONTH_OF_YEAR, TextStyle.FULL).toFormatter(l);
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'b': 
                // Locale-specific abbreviated month name, e.g. "Jan", "Feb", same as h.
            case 'h': {
                // Locale-specific abbreviated month name, e.g. "Jan", "Feb", same as b.
                DateTimeFormatter dtf = new DateTimeFormatterBuilder().appendText(ChronoField.MONTH_OF_YEAR, TextStyle.SHORT).toFormatter(l);
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'A': 
                // Locale-specific full name of the day of the week, e.g. "Sunday", "Monday"
                taToStr = (sb, ta) -> sb.append(DayOfWeek.from(ta).getDisplayName(TextStyle.FULL, l));
                zoned = true;
                chronologyCheck = false;
                break;
            case 'a':
                // Locale-specific short name of the day of the week, e.g. "Sun", "Mon"
                taToStr = (sb, ta) -> sb.append(DayOfWeek.from(ta).getDisplayName(TextStyle.SHORT, l));
                zoned = true;
                chronologyCheck = false;
                break;
            case 'C':
                // Four-digit year divided by 100, formatted as two digits with leading zero as necessary, i.e. 00 - 99
                taToStr = formatTemporalAccessor(l, "00", i -> i.getLong(ChronoField.YEAR_OF_ERA) / 100);
                zoned = true;
                chronologyCheck = true;
                break;
            case 'Y':
                // Year, formatted as at least four digits with leading zeros as necessary, e.g. 0092 equals 92 CE for the Gregorian calendar.
                taToStr = formatTemporalAccessor(l, "0000", ChronoField.YEAR_OF_ERA);
                zoned = true;
                chronologyCheck = true;
                break;
            case 'y':
                // Last two digits of the year, formatted with leading zeros as necessary, i.e. 00 - 99.
                taToStr = formatTemporalAccessor(l, "00", i -> i.getLong(ChronoField.YEAR_OF_ERA) % 100);
                zoned = true;
                chronologyCheck = true;
                break;
            case 'j':
                // Day of year, formatted as three digits with leading zeros as necessary, e.g. 001 - 366 for the Gregorian calendar.
                taToStr = formatTemporalAccessor(l, "000", ChronoField.DAY_OF_YEAR);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'm':
                // Month, formatted as two digits with leading zeros as necessary, i.e. 01 - 13.
                taToStr = formatTemporalAccessor(l, "00", ChronoField.MONTH_OF_YEAR);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'd':
                // Day of month, formatted as two digits with leading zeros as necessary, i.e. 01 - 31
                taToStr = formatTemporalAccessor(l, "00", ChronoField.DAY_OF_MONTH);
                zoned = true;
                chronologyCheck = false;
               break;
            case 'e':
                // Day of month, formatted as two digits, i.e. 1 - 31.
                taToStr = formatTemporalAccessor(l, "#0", ChronoField.DAY_OF_MONTH);
                zoned = true;
                chronologyCheck = false;
                break;
            case 'V':
                // The week number of the year (Monday as the first day of the week) as a decimal number (01-53).  If the week
                // containing January 1 has four or more days in the new year, then it is week 1; otherwise it is the last week
                // of the previous year, and the next week is week 1. Defined in ISO-8601
                taToStr = formatTemporalAccessor(l, "00", WeekFields.ISO.weekOfWeekBasedYear());
                zoned = true;
                chronologyCheck = false;
               break;
            case 'R':{
                // Time formatted for the 24-hour clock as "%tH:%tM"
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'T': {
                // Time formatted for the 24-hour clock as "%tH:%tM:%tS".
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'r':{
                // Time formatted for the 12-hour clock as "%tI:%tM:%tS %Tp". The location of the morning or afternoon marker ('%Tp') may be locale-dependent.
                DateTimeFormatter dtf1 = DateTimeFormatter.ofPattern("hh:mm:ss", l).withDecimalStyle(DecimalStyle.of(l));
                DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("a", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> {
                    dtf1.formatTo(ta, sb);
                    sb.append(' ');
                    String tzStr =  dtf2.format(ta).toUpperCase();
                    sb.append(tzStr);
                };
                zoned = true;
                chronologyCheck = false;
                break;
            }
            case 'D': {
                // Date formatted as "%tm/%td/%ty
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM/dd/yy", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = true;
                break;
            }
            case 'F': {
                // ISO 8601 complete date formatted as "%tY-%tm-%td".
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = true;
                break;
            }
            case 'c': {
                // Date and time formatted as "%ta %tb %td %tT %tZ %tY", e.g. "Sun Jul 20 16:17:00 EDT 1969".
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("eee MMM dd HH:mm:ss zz yyyy", l).withDecimalStyle(DecimalStyle.of(l));
                taToStr = (sb, ta) -> dtf.formatTo(ta, sb);
                zoned = true;
                chronologyCheck = true;
                break;
            }
            default:
                throw new IllegalArgumentException("Unreconized date/time format: '" + timeFormat + "'");
            }
        }

        private BiConsumer<StringBuffer, TemporalAccessor> formatTemporalAccessor(Locale l, String formatPattern, TemporalField field) {
            ThreadLocal<DecimalFormat> nf = getDecimalFormat(formatPattern, l);
            return (sb, ta) -> nf.get().format(ta.get(field), sb, INSTANCE);
        }

        private BiConsumer<StringBuffer, TemporalAccessor> formatTemporalAccessor(Locale l, String formatPattern, TemporalQuery<Long> transformd) {
            ThreadLocal<DecimalFormat> nf = getDecimalFormat(formatPattern, l);
            return (sb, ta) -> nf.get().format(transformd.queryFrom(ta), sb, INSTANCE);
        }

        private ThreadLocal<DecimalFormat> getDecimalFormat(String pattern, Locale locale) {
            return ThreadLocal.withInitial(() -> new DecimalFormat(pattern, DecimalFormatSymbols.getInstance(locale)));
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
            if (obj instanceof Instant && zoned){
                Instant i = (Instant) obj;
                return withCalendarSystem(i.atZone(etz));
            } else if (obj instanceof ZonedDateTime && tz != null){
                ZonedDateTime zdt = (ZonedDateTime) obj;
                return zdt.withZoneSameInstant(etz);
            } else if (obj instanceof TemporalAccessor){
                return (TemporalAccessor) obj;
            } else {
                throw new IllegalArgumentException("Not a date/time argument");
            }
        }

        @Override
        public StringBuffer format(Object obj, StringBuffer toAppendTo,
                                   FieldPosition pos) {
            if ( ! (obj instanceof Date) && ! (obj instanceof TemporalAccessor)) {
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

        @Override
        public Object parseObject(String source, ParsePosition pos) {
            throw new UnsupportedOperationException("Can't parse");
        }
    }

    private static final Pattern varregexp = Pattern.compile("^(?<before>.*?(?=(?:\\$\\{)|\\{|'))(?:\\$\\{(?<varname>#?[\\w\\.-]+)?(?<format>%[^}]+)?\\}|(?:(?<curlybraces>\\{.*\\})|(?<quote>')))(?<after>.*)$", Pattern.DOTALL);
    private static final Pattern formatSpecifier = Pattern.compile("^(?<flag>[-#+ 0,(]*)?(?<length>\\d+)?(?:\\.(?<precision>\\d+))?(?:(?<istime>[tT])(?:\\<(?<tz>.*)\\>)?)?(?<conversion>[a-zA-Z%])(?::(?<locale>.+))?$", Pattern.DOTALL);
    private static final Pattern arrayIndex = Pattern.compile("#(?<index>\\d+)");
    private static final String lineseparator = System.lineSeparator();

    private static final Logger logger = LogManager.getLogger();

    private final Map<Object, Integer> mapper = new LinkedHashMap<>();
    private final MessageFormat mf;

    private ZoneId tz = ZoneId.systemDefault();
    private Locale locale;
    private final String format;
    @Getter
    private final boolean empty;

    public VarFormatter(String format) {
        this(format, Locale.getDefault());
    }

    /**
     * @param format
     * @param l
     * @throws IllegalArgumentException
     */
    public VarFormatter(String format, Locale l) {
        logger.trace("new format: {}", format);
        this.format = format;
        locale = l;
        List<String> formats = new ArrayList<>();
        // Convert the pattern to a MessageFormat which is compiled and be reused
        String pattern = findVariables(new StringBuilder(), format, 0, formats).toString();
        try {
            mf = new MessageFormat(pattern, l);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(String.format("Can't format %s, locale %s: %s", format, l, ex.getMessage()), ex);
        }
        for (int i = 0; i < mf.getFormats().length; i++) {
            mf.setFormat(i, resolveFormat(formats.get(i)));
        }
        if (mapper.size() != 0) {
            empty = false;
            mapper.keySet().stream().reduce((i,j) ->  {
                if (i.getClass() != j.getClass()) {
                    throw new IllegalArgumentException("Can't mix indexed with object resolution");
                } else {
                    return j;
                }
            });
        } else {
            empty = true;
        }
    }

    public String argsFormat(Object... arg) throws IllegalArgumentException {
        return format(arg);
    }

    @SuppressWarnings("unchecked")
    public String format(Object arg) throws IllegalArgumentException {
        Map<String, Object> variables;
        Object mapperType = mapper.keySet().stream().findAny().orElse("");
        if (( mapperType instanceof Number) && ! ( arg instanceof List || arg instanceof Object[])) {
            throw new IllegalArgumentException("Given a non-list to a format expecting only a list or an array");
        } else if (arg instanceof Map) {
            variables = (Map<String, Object>) arg;
        } else {
            variables = Collections.emptyMap();
        }
        Object[] resolved = new Object[mapper.size()];
        for(Map.Entry<Object, Integer> mapping: mapper.entrySet()) {
            if (".".equals(mapping.getKey())) {
                resolved[mapping.getValue()] = checkArgType(arg);
                continue;
            }
            if (mapperType instanceof Number && ! arg.getClass().isArray()) {
                int i = ((Number) mapping.getKey()).intValue();
                int j = ((Number) mapping.getValue()).intValue();
                List<Object> l = (List<Object>) arg;
                if (j > l.size()) {
                    throw new IllegalArgumentException("index out of range");
                }
                resolved[i] = checkArgType(l.get(j - 1));
            } else if (mapperType instanceof Number && arg instanceof Object[]) {
                int i = ((Number) mapping.getKey()).intValue();
                int j = ((Number) mapping.getValue()).intValue();
                Object[] a = (Object[]) arg;
                if (j > a.length) {
                    throw new IllegalArgumentException("index out of range");
                }
                resolved[i] = checkArgType(a[j - 1]);
            } else {
                String[] path = mapping.getKey().toString().split("\\.");
                if (path.length == 1) {
                    // Only one element in the key, just use it
                    if (! variables.containsKey(mapping.getKey())) {
                        throw new IllegalArgumentException("invalid values for format key " + mapping.getKey());
                    }
                    resolved[mapping.getValue()] = checkArgType(variables.get(mapping.getKey()));
                } else {
                    // Recurse, variables written as "a.b.c" are paths in maps
                    Map<String, Object> current = variables;
                    String key = path[0];
                    for (int i = 0; i < path.length - 1; i++) {
                        Map<String, Object> next = (Map<String, Object>) current.get(key);
                        if (next == null || ! (next instanceof Map) ) {
                            throw new IllegalArgumentException("invalid values for format key " + mapping.getKey());
                        }
                        current = next;
                        key = path[i + 1];
                    }
                    if (current != null) {
                        resolved[mapping.getValue()] = checkArgType(current.get(key));
                    }
                }
            }
        }
        return mf.format(resolved, new StringBuffer(), INSTANCE).toString();
    }

    private static final Locale LOCALEJAPANESERA = Locale.forLanguageTag("ja-JP-u-ca-japanese-x-lvariant-JP");
    private static final Locale LOCALETHAIERA = Locale.forLanguageTag("ja-JP-u-ca-japanese-x-lvariant-JP");

    private Object checkArgType(Object arg) {
        if (arg instanceof Date) {
            Date d = (Date) arg;
            return d.toInstant();
        } else if (arg instanceof Calendar) {
            // Because Calendar type hierarchy is inconsistent and some class are not public
            Calendar c = (Calendar) arg;
            switch (c.getCalendarType()) {
            case "gregory":
                GregorianCalendar gc = (GregorianCalendar) c;
                return gc.toZonedDateTime();
            case "japanese":
                return resolveWithEra(LOCALEJAPANESERA,
                                      ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
            case "buddhist":
                return resolveWithEra(LOCALETHAIERA,
                                      ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
            default:
                return resolveWithEra(Locale.getDefault(),
                        ZonedDateTime.ofInstant(c.toInstant(), c.getTimeZone().toZoneId()));
            }
        } else if (arg == null || ! arg.getClass().isArray()) {
            return arg;
        } else if (arg instanceof byte[]) {
            return Arrays.toString((byte[])arg);
        } else if (arg instanceof short[]) {
            return Arrays.toString((short[])arg);
        } else if (arg instanceof int[]) {
            return Arrays.toString((int[])arg);
        } else if (arg instanceof long[]) {
            return Arrays.toString((long[])arg);
        } else if (arg instanceof float[]) {
            return Arrays.toString((float[])arg);
        } else if (arg instanceof double[]) {
            return Arrays.toString((double[])arg);
        } else if (arg instanceof boolean[]) {
            return Arrays.toString((boolean[])arg);
        } else if (arg instanceof char[]) {
            return Arrays.toString((char[])arg);
        } else {
            return Arrays.deepToString((Object[])arg);
        }
    }

    private StringBuilder findVariables(StringBuilder buffer, String in, int last, List<String> formats) {
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
                buffer.append("'" + curlybraces + "'");
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
                    mapper.put(last++, i);
                } else if (! mapper.containsKey(varname)) {
                    index = last;
                    mapper.put(varname, last++);
                } else {
                    index = mapper.get(varname);
                }
                buffer.append("{" + index + "}");
            }
            findVariables(buffer, after, last, formats);
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
            if(flagStr == null) {
                flagStr = "";
            }
            Flags flags = new Flags(flagStr);
            String isTime =  m.group("istime");

            String conversionStr = m.group("conversion");

            final Character timeFormat;
            final ZoneId tz;
            if (isTime != null && ! isTime.isEmpty()) {
                timeFormat = conversionStr.charAt(0);
                conversionStr = isTime;
                String tzStr = m.group("tz");
                if (tzStr != null && ! tzStr.isEmpty()) {
                    tz = ZoneId.of(tzStr);
                } else if (tzStr != null && tzStr.isEmpty()) {
                    tz = this.tz;
                } else {
                    tz = null;
                }
            } else {
                timeFormat = null;
                tz = null;
            }

            boolean isUpper = conversionStr.toUpperCase(locale).equals(conversionStr);
            char conversion = conversionStr.toLowerCase(locale).charAt(0);

            final Function<String, String> cut = i -> precision < 0 ? i : i.substring(0, precision);
            switch(conversion) {
            case 'b': return new NonParsingFormat(Locale.getDefault(), isUpper, i -> cut.apply(i == null ? "false" : (i instanceof Boolean) ? i.toString() : "true"));
            case 's': return new NonParsingFormat(Locale.getDefault(), isUpper, i -> cut.apply(i.toString()) );
            case 'h': return new NonParsingFormat(Locale.getDefault(), isUpper, i -> cut.apply(i == null ? "null" : Integer.toHexString(i.hashCode())));
            case 'c': return new NonParsingFormat(Locale.getDefault(), isUpper, i -> (i instanceof Character) ? i.toString() : "null");
            case 'd': {Format f = numberFormat(locale, conversion, flags, true, length, precision, isUpper); return new NonParsingFormat(locale, false, f::format);}
            case 'o': return new NonDecimalFormat(locale, 8, isUpper, flags, precision);
            case 'x': return new NonDecimalFormat(locale, 16, isUpper, flags, precision);
            case 'e': {Format f = numberFormat(locale, conversion, flags, false, length, precision, isUpper); return new NonParsingFormat(locale, false, f::format);}
            case 'f': {Format f = numberFormat(locale, conversion, flags, false, length, precision, isUpper); return new NonParsingFormat(locale, false, f::format);}
            case 'g': {Format f = numberFormat(locale, conversion, flags, false, length, precision, isUpper); return new NonParsingFormat(locale, false, f::format);}
            case 'a': {Format f = numberFormat(locale, conversion, flags, false, length, precision, isUpper); return new NonParsingFormat(locale, false, f::format);}
            case 't': return new ExtendedDateFormat(locale, timeFormat, tz, isUpper);
            case '%': return new NonParsingFormat(Locale.getDefault(), false, i -> "%");
            case 'n': return new NonParsingFormat(Locale.getDefault(), false, i -> lineseparator);
            default: throw new IllegalArgumentException("Invalid format specifier: " + format);
            }
        } else {
            throw new IllegalArgumentException(format);
        }
    }

    private Format numberFormat(Locale l, char conversion, Flags flags, boolean integer, int length, int precision, boolean isUpper) {
        precision = (precision == -1 ? 6 : precision);
        DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance(l);
        symbols.setExponentSeparator( isUpper ? "E" : "e");
        symbols.setDigit(flags.zeropadded ? '0' : '#');
        int fixed = (integer ? length : length - precision);
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
            df.setPositivePrefix("+");;
        }
        if (! integer) {
            df.setMinimumFractionDigits(precision);
        }
        if (symbols.getDigit() == '0') {
            df.setMinimumIntegerDigits(fixed);
        }
        if (flags.leftjustified) {
            return new LeftJustifyFormat(df, length);
        } else {
            return new RightJustifyFormat(df, length);
        }

    }

    @Override
    public String toString() {
        return this.format;
    }

    static ChronoZonedDateTime<? extends ChronoLocalDate> resolveWithEra(Locale locale, ZonedDateTime timePoint) {
        switch (locale.getLanguage()) {
        case "th":
            if ("TH".equals(locale.getCountry())) {
                return ThaiBuddhistDate.from(timePoint).atTime(timePoint.toLocalTime()).atZone(timePoint.getZone());
            } else {
                return timePoint;
            }
        case "ja":
            if ("japanese".equals(locale.getUnicodeLocaleType("ca"))) {
                return JapaneseDate.from(timePoint).atTime(timePoint.toLocalTime()).atZone(timePoint.getZone());
            } else {
                return timePoint;
            }
        default:
            return timePoint;
        }
    }

}
