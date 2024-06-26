package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.axibase.date.DatetimeProcessorUtil.appendNumberWithFixedPositions;

/**
 * This class resolves creates for Axibase-supported datetime syntax. Each DatetimeProcessor object is immutable,
 * so consider caching them for better performance in client application.
 */
public class PatternResolver {

    private static final Pattern OPTIMIZED_PATTERN = Pattern.compile("yyyy-MM-dd('.'|.)HH:mm:ss(\\.S{1,9})?(Z{1,2}|X{1,5}|x{1,5})?");
    private static final Pattern RFC822_PATTERN = Pattern.compile("(eee,? +)?(d{1,2}) +MMM( +yyyy)? +HH:mm:ss(\\.S{1,9})?( Z{1,2}|X{1,5}|x{1,5})?");
    private static final Pattern RFC3164_PATTERN = Pattern.compile("MMM +(d{1,2})( yyyy)? HH:mm:ss(\\.S{1,9})?( Z{1,2}|X{1,5}|x{1,5})?");
    private static final Pattern DISABLE_LENIENT_MODE = Pattern.compile("^(?:u+|[^u]*u{1,3}[A-Za-z0-9]+)$");

    public static DatetimeProcessor createNewFormatter(String pattern) {
        return createNewFormatter(pattern, ZoneId.systemDefault());
    }

    public static DatetimeProcessor createNewFormatter(String pattern, ZoneId zoneId) {
        return createNewFormatter(pattern, zoneId, OnMissingDateComponentAction.SET_ZERO);
    }

    public static DatetimeProcessor createNewFormatter(String pattern, ZoneId zoneId, OnMissingDateComponentAction onMissingDateComponent) {
        DatetimeProcessor result;
        if (NamedPatterns.SECONDS.equalsIgnoreCase(pattern)) {
           result = new DatetimeProcessorUnixSeconds(zoneId);
        } else if (NamedPatterns.MILLISECONDS.equalsIgnoreCase(pattern)) {
           result = new DatetimeProcessorUnixMillis(zoneId);
        } else if (NamedPatterns.NANOSECONDS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorUnixNano(zoneId);
        } else if (NamedPatterns.ISO.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(3, resolveZoneOffset("ZZ"), zoneId, 'T');
        } else if (NamedPatterns.ISO_SECONDS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(0, resolveZoneOffset("ZZ"), zoneId, 'T');
        } else if (NamedPatterns.ISO_NANOS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(9, resolveZoneOffset("ZZ"), zoneId, 'T');
        } else if (NamedPatterns.RFC822.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorRfc822(true, 1, true, 0, resolveZoneOffset("Z"));
        } else if (NamedPatterns.RFC3164.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorRfc3164(1, true, 0, resolveZoneOffset("Z"));
        } else {
            result = createFromDynamicPattern(pattern, zoneId, onMissingDateComponent);
        }
        return result;
    }

    private static DatetimeProcessor createFromDynamicPattern(String pattern, ZoneId zoneId, OnMissingDateComponentAction onMissingDateComponentAction) {
        Matcher matcherIso8601 = OPTIMIZED_PATTERN.matcher(pattern);
        if (matcherIso8601.matches()) {
            int fractions = stringLength(matcherIso8601.group(2)) - 1;
            char delimitor;
            if (matcherIso8601.group(1).length() == 3) {
                delimitor = matcherIso8601.group(1).charAt(1);
            } else {
                delimitor = matcherIso8601.group(1).charAt(0);
            }
            return new DatetimeProcessorIso8601(fractions, resolveZoneOffset(matcherIso8601.group(3)), zoneId, delimitor);
        }
        Matcher matcherRfc822 = RFC822_PATTERN.matcher(pattern);
        if (matcherRfc822.matches()) {
            int dayLength = matcherRfc822.group(2).length();
            int fractions = stringLength(matcherRfc822.group(4)) - 1;
            boolean withYear = matcherRfc822.group(3) != null;
            AppendOffset appendOffset = Optional.ofNullable(matcherRfc822.group(5))
                                                .map(s -> s.substring(1))
                                                .map(PatternResolver::resolveZoneOffset)
                                                .orElse(null);
            return new DatetimeProcessorRfc822(true, dayLength, withYear, fractions, appendOffset);
        }
        Matcher matcherRfc3164 = RFC3164_PATTERN.matcher(pattern);
        if (matcherRfc3164.matches()) {
            int dayLength = matcherRfc3164.group(1).length();
            int fractions = stringLength(matcherRfc3164.group(3)) - 1;
            boolean withYear = matcherRfc3164.group(2) != null;
            AppendOffset appendOffset = Optional.ofNullable(matcherRfc3164.group(4))
                                                .map(s -> s.substring(1))
                                                .map(PatternResolver::resolveZoneOffset)
                                                .orElse(null);
            return new DatetimeProcessorRfc3164(dayLength, withYear, fractions, appendOffset);
        }
        String preprocessedPattern = preprocessPattern(pattern);
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                .parseCaseInsensitive();
        if (enableLenient(preprocessedPattern)) {
            builder.parseLenient();
        }
        DateTimeFormatter dateTimeFormatter = builder
                .appendPattern(preprocessedPattern)
                .toFormatter(Locale.US)
                .withResolverStyle(ResolverStyle.STRICT);
        return new DatetimeProcessorCustom(dateTimeFormatter, zoneId, onMissingDateComponentAction);
    }

    private static int stringLength(String value) {
        return value == null ? 0 : value.length();
    }

    private static boolean enableLenient(String pattern) {
        return !DISABLE_LENIENT_MODE.matcher(pattern).matches();
    }

    /**
     * Replace documented FDF symbols to their JSR-310 analogs. The conversions are performed:
     * unquoted T -> quoted T
     * u -> ccccc (day of week starting from Monday)
     * Z -> XX (zone offset in RFC format: +HHmm, Z for UTC)
     * ZZ -> XXX (zone offset in ISO format: +HH:mm, Z for UTC)
     * ZZZ -> VV (zone id)
     * @param pattern time formatting pattern
     * @return JSR-310 compatible pattern
     */
    private static String preprocessPattern(String pattern) {
        int length = pattern.length();
        boolean insideQuotes = false;
        StringBuilder sb = new StringBuilder(pattern.length() + 5);
        DateFormatParsingState state = new DateFormatParsingState();
        for (int i = 0; i < length; i++) {
            char c = pattern.charAt(i);
            if (c != 'u') {
                state.updateU(sb);
            }
            if (c != 'Z') {
                state.updateZ(sb);
            }
            switch (c) {
                case '\'':
                    insideQuotes = !insideQuotes;
                    sb.append(c);
                    break;
                case 'T':
                    if (!insideQuotes) {
                        sb.append("'T'");
                    } else {
                        sb.append(c);
                    }
                    break;
                case 'Z':
                    if (!insideQuotes) {
                        ++state.zCount;
                    }
                    sb.append(c);
                    break;
                case 'u':
                    if (!insideQuotes) {
                        ++state.uCount;
                    }
                    sb.append(c);
                    break;
                case 'y':
                    sb.append('u');
                    break;
                default:
                    sb.append(c);
            }
        }
        state.updateU(sb);
        state.updateZ(sb);
        return sb.toString();
    }

    private static class DateFormatParsingState {
        private int zCount = 0;
        private int uCount = 0;

        private void updateU(StringBuilder sb) {
            if (uCount > 0) {
                sb.setLength(sb.length() - uCount);
                sb.append("0".repeat(Math.max(0, uCount - 1)));
                sb.append("ccccc");
            }
            uCount = 0;
        }

        private void updateZ(StringBuilder sb) {
            if (zCount > 0 && zCount <= 3) {
                sb.setLength(sb.length() - zCount);
                if (zCount == 1) {
                    sb.append("XX");
                } else if (zCount == 2) {
                    sb.append("XXX");
                } else {
                    sb.append("VV");
                }
            }
            zCount = 0;
        }
    }

    private static AppendOffset resolveZoneOffset(String pattern) {
        BiFunction<ZoneId, Instant, Integer> resolveOffset = (o, i) -> o.getRules().getOffset(i).getTotalSeconds();
        if (pattern == null) {
            return null;
        } else {
            switch (pattern) {
            case "X":
                return (sb, offset, instant) -> appendFormattedSecondOffset(true, 1, ' ', resolveOffset.apply(offset, instant), sb);
            case "Z":
            case "XX":
                return (sb, offset, instant) -> appendFormattedSecondOffset(true, 2, ' ', resolveOffset.apply(offset, instant), sb);
            case "ZZ":
            case "XXX":
                return (sb, offset, instant) -> appendFormattedSecondOffset(true, 2, ':', resolveOffset.apply(offset, instant), sb);
            case "XXXX":
                return (sb, offset, instant) -> appendFormattedSecondOffset(true, 3, ' ', resolveOffset.apply(offset, instant), sb);
            case "XXXXX":
                return (sb, offset, instant) -> appendFormattedSecondOffset(true, 3, ':', resolveOffset.apply(offset, instant), sb);
            case "x":
                return (sb, offset, instant) -> appendFormattedSecondOffset(false, 1, ' ', resolveOffset.apply(offset, instant), sb);
            case "xx":
                return (sb, offset, instant) -> appendFormattedSecondOffset(false, 2, ' ', resolveOffset.apply(offset, instant), sb);
            case "xxx":
                return (sb, offset, instant) -> appendFormattedSecondOffset(false, 2, ':', resolveOffset.apply(offset, instant), sb);
            case "xxxx":
                return (sb, offset, instant) -> appendFormattedSecondOffset(false, 3, ' ', resolveOffset.apply(offset, instant), sb);
            case "xxxxx":
                return (sb, offset, instant) -> appendFormattedSecondOffset(false, 3, ':', resolveOffset.apply(offset, instant), sb);
            default:
                return (sb, offset, instant) -> sb;
            }
        }
    }

    static StringBuilder appendFormattedSecondOffset(boolean zuluTime, int rank, char separator, int offsetSeconds, StringBuilder sb) {
        if (offsetSeconds == 0 && zuluTime) {
            return sb.append('Z');
        } else {
            sb.append(offsetSeconds < 0 ? '-' : '+');
            int absSeconds = Math.abs(offsetSeconds);
            if (rank >= 1) {
                appendNumberWithFixedPositions(sb, absSeconds / 3600, 2);
            }
            if (rank >= 2) {
                if (separator == ':') {
                    sb.append(':');
                }
                appendNumberWithFixedPositions(sb, (absSeconds / 60) % 60, 2);
            }
            if (rank >= 3) {
                if (separator == ':') {
                    sb.append(':');
                }
                appendNumberWithFixedPositions(sb, absSeconds % 60, 2);
            }
            return sb;
        }
    }

}
