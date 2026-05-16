package loghub.datetime;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalQueries;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static loghub.datetime.DatetimeProcessorUtil.appendNumberWithFixedPositions;

/**
 * This class resolves creates for Axibase-supported datetime syntax. Each DatetimeProcessor object is immutable,
 * so consider caching them for better performance in client application.
 */
class PatternResolver {
    static final Set<String> VALID_ZONE_FORMATTERS = Set.of(
            "O", "OOOO",                        // localized zone-offset:  GMT+8; GMT+08:00; UTC-08:00
            "VV",                               // time-zone ID: America/Los_Angeles: Z; -08:30
            "X", "XX", "XXX", "XXXX", "XXXXX",  // zone-offset 'Z' for zero: Z; -08; -0830; -08:30; -083015; -08:30:15
            "Z", "ZZ", "ZZZ", "ZZZZ", "ZZZZZ",  // zone-offset: +0000; -0800; -08:00
            "v", "vvvv",                        // generic time-zone name: Pacific Time; PT
            "x", "xx", "xxx", "xxxx", "xxxxx",  // zone-offset: +0000; -08; -0830; -08:30; -083015; -08:30:15
            "z", "zz", "zzz", "zzzz"            // time-zone name: Pacific Standard Time; PST
    );
    private static final String VALID_ZONE_PATTERNS = "(" + String.join("|", VALID_ZONE_FORMATTERS) + ")";

    private static final Pattern IO8601_PATTERN = buildPattern("yyyy-MM-dd('.'|.)HH:mm:ss(([.,])S{1,9})?(%s)?");
    private static final Pattern RFC822_PATTERN = buildPattern("(eee,?\\s+)?(d{1,2})\\s+MMM(\\s+yyyy)?\\s+HH:mm:ss(\\.S{1,9})?(\\s+%s)?");
    private static final Pattern RFC3164_PATTERN = buildPattern("MMM\\s+(d{1,2})(\\s+yyyy)?\\s+HH:mm:ss(\\.S{1,9})?(\\s+%s)?");

    private static Pattern buildPattern(String basePattern) {
        String effectivePattern = String.format("\\s*" + basePattern + "\\s*", VALID_ZONE_PATTERNS);
        return Pattern.compile(effectivePattern);
    }

    static DatetimeProcessor createNewFormatter(String pattern) {
        DatetimeProcessor result;
        if (NamedPatterns.SECONDS.equalsIgnoreCase(pattern)) {
           result = new DatetimeProcessorUnixSeconds();
        } else if (NamedPatterns.MILLISECONDS.equalsIgnoreCase(pattern)) {
           result = new DatetimeProcessorUnixMillis();
        } else if (NamedPatterns.NANOSECONDS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorUnixNano();
        } else if (NamedPatterns.ISO.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(3, resolveZoneOffset("XXXXX"), zoneOffsetResolver("XXXXX"), 'T', '.');
        } else if (NamedPatterns.ISO_SECONDS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(0, resolveZoneOffset("XXXXX"), zoneOffsetResolver("XXXXX"), 'T', '.');
        } else if (NamedPatterns.ISO_NANOS.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorIso8601(9, resolveZoneOffset("XXXXX"), zoneOffsetResolver("XXXXX"), 'T', '.');
        } else if (NamedPatterns.RFC822.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorRfc822(true, 1, true, 0, resolveZoneOffset("Z"), zoneOffsetResolver("Z"));
        } else if (NamedPatterns.RFC3164.equalsIgnoreCase(pattern)) {
            result = new DatetimeProcessorRfc3164(1, false, 0, null, zoneOffsetResolver("Z"));
        } else {
            result = createFromDynamicPattern(pattern);
        }
        return result;
    }

    private static DatetimeProcessor createFromDynamicPattern(String pattern) {
        Matcher matcherIso8601 = IO8601_PATTERN.matcher(pattern);
        if (matcherIso8601.matches()) {
            int fractions = stringLength(matcherIso8601.group(2)) - 1;
            char delimitor;
            if (matcherIso8601.group(1).length() == 3) {
                delimitor = matcherIso8601.group(1).charAt(1);
            } else {
                delimitor = matcherIso8601.group(1).charAt(0);
            }
            char decimalMark;
            if (fractions > 0) {
                decimalMark = matcherIso8601.group(3).charAt(0);
            } else {
                decimalMark = '.';
            }
            return new DatetimeProcessorIso8601(fractions, resolveZoneOffset(matcherIso8601.group(4)), zoneOffsetResolver(matcherIso8601.group(5)), delimitor, decimalMark);
        }
        Matcher matcherRfc822 = RFC822_PATTERN.matcher(pattern);
        if (matcherRfc822.matches()) {
            int dayLength = matcherRfc822.group(2).length();
            int fractions = stringLength(matcherRfc822.group(4)) - 1;
            boolean withYear = matcherRfc822.group(3) != null;
            AppendOffset appendOffset = Optional.ofNullable(matcherRfc822.group(6))
                                                .map(PatternResolver::resolveZoneOffset)
                                                .orElse(null);
            ParseTimeZone ptz = Optional.ofNullable(matcherRfc822.group(6))
                                        .map(PatternResolver::zoneOffsetResolver)
                                        .orElse(null);
            return new DatetimeProcessorRfc822(true, dayLength, withYear, fractions, appendOffset, ptz);
        }
        Matcher matcherRfc3164 = RFC3164_PATTERN.matcher(pattern);
        if (matcherRfc3164.matches()) {
            int dayLength = matcherRfc3164.group(1).length();
            int fractions = stringLength(matcherRfc3164.group(3)) - 1;
            boolean withYear = matcherRfc3164.group(2) != null;
            AppendOffset appendOffset = Optional.ofNullable(matcherRfc3164.group(5))
                                                .map(PatternResolver::resolveZoneOffset)
                                                .orElse(null);
            ParseTimeZone ptz = Optional.ofNullable(matcherRfc3164.group(5))
                                                .map(PatternResolver::zoneOffsetResolver)
                                                .orElse(null);
            return new DatetimeProcessorRfc3164(dayLength, withYear, fractions, appendOffset, ptz);
        }
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder()
                .parseCaseInsensitive().parseLenient();
        DateTimeFormatter dateTimeFormatter = builder
                .appendPattern(pattern)
                .toFormatter(Locale.US)
                .withResolverStyle(ResolverStyle.STRICT);
        return new DatetimeProcessorCustom(dateTimeFormatter);
    }

    private static int stringLength(String value) {
        return value == null ? 0 : value.length();
    }

    static AppendOffset resolveZoneOffset(String pattern) {
        if (pattern == null) {
            return null;
        } else {
            switch (pattern) {
            case "Z":
                return (sb, zdt) -> appendFormattedSecondOffset("+0000", 2, false, ' ', zdt, sb);
            case "ZZ":
                return (sb, zdt) -> appendFormattedSecondOffset("+0000", 2, false, ' ', zdt, sb);
            case "X":
                return (sb, zdt) -> appendFormattedSecondOffset("Z", 2, true, ' ', zdt, sb);
            case "XX":
                return (sb, zdt) -> appendFormattedSecondOffset("Z", 2, false, ' ', zdt, sb);
            case "XXX":
                return (sb, zdt) -> appendFormattedSecondOffset("Z", 2, false, ':', zdt, sb);
            case "XXXX":
                return (sb, zdt) -> appendFormattedSecondOffset("Z", 3, true, ' ', zdt, sb);
            case "XXXXX":
                return (sb, zdt) -> appendFormattedSecondOffset("Z", 3, true, ':', zdt, sb);
            case "x":
                return (sb, zdt) -> appendFormattedSecondOffset("+00", 2, true, ' ', zdt, sb);
            case "xx":
                return (sb, zdt) -> appendFormattedSecondOffset("+0000", 2, false, ' ', zdt, sb);
            case "xxx":
                return (sb, zdt) -> appendFormattedSecondOffset("+00:00", 2, false, ':', zdt, sb);
            case "xxxx":
                return (sb, zdt) -> appendFormattedSecondOffset("+0000", 3, true, ' ', zdt, sb);
            case "xxxxx":
                return (sb, zdt) -> appendFormattedSecondOffset("+00:00", 3, true, ':', zdt, sb);
            default:
                return new AppendOffset.PatternAppendOffset(pattern);
            }
        }
    }

    static StringBuilder appendFormattedSecondOffset(String zuluTime, int rank, boolean minimum, char separator, ZonedDateTime zdt, StringBuilder sb) {
        int offsetSeconds = zdt.query(TemporalQueries.offset()).getTotalSeconds();
        if (offsetSeconds == 0 && ! zuluTime.isEmpty()) {
            return sb.append(zuluTime);
        } else {
            sb.append(offsetSeconds < 0 ? '-' : '+');
            int absSeconds = Math.abs(offsetSeconds);
            int minutes = (absSeconds / 60) % 60;
            int seconds = absSeconds % 60;
            appendNumberWithFixedPositions(sb, absSeconds / 3600, 2);
            if (rank >= 2 && ((! minimum) || (minutes != 0 && seconds != 0))) {
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

    static ParseTimeZone zoneOffsetResolver(String pattern) {
        if (pattern != null && VALID_ZONE_FORMATTERS.contains(pattern)) {
            switch (pattern) {
            case "v":
            case "vvvv":
            case "z":
            case "zz":
            case "zzz":
            case "zzzz": {
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern(pattern);
                return (ctx, ot, dzid) -> dtf.parse(ctx.findWord(), TemporalQueries.zoneId());
            }
            default:
                return ParsingContext::extractZoneId;
            }
        } else {
            return (ctx, ot, dzid) -> dzid;
        }
    }

}
