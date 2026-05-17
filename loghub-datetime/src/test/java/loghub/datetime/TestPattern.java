package loghub.datetime;

import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestPattern {

    private DatetimeProcessor getProcessor(String pattern) {
        return PatternResolver.createNewFormatter(pattern)
                              .withLocale(Locale.ENGLISH)
                              .withDefaultZone(ZoneId.of("Europe/London"));
    }

    private void runTest(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        DatetimeProcessor thisProcessor = getProcessor(pattern);
        toparse.forEach(e -> Assertions.assertEquals(e.getValue(), thisProcessor.parse(e.getKey()).toString(), String.format("When parsing \"%s\"", e.getKey())));
        toPrint.forEach(e -> Assertions.assertEquals(e.getValue(), thisProcessor.print(e.getKey()), String.format("When printing \"%s\" with \"%s\"", e.getKey(), pattern)));
        fails.forEach(e -> {
            DateTimeParseException dtpe = Assertions.assertThrows(DateTimeParseException.class, () -> thisProcessor.parse(e.getKey()));
            Assertions.assertEquals(e.getValue(), dtpe.getMessage());
        });
    }

    // -------------------------------------------------------------------------
    // rfc822
    // -------------------------------------------------------------------------

    static Stream<Arguments> rfc822Provider() {
        return Stream.of(
                Arguments.of("rfc822",
                        List.of(
                                Map.entry("03 May 2024 12:56:29 +01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry("03 May 2024 12:56:29 CET", "2024-05-03T12:56:29+02:00[CET]"),
                                Map.entry("XXX, 03 May 2024 12:56:29 CET", "2024-05-03T12:56:29+02:00[CET]"),
                                Map.entry("Tue, 3 Jun 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10"),
                                // need to check with sunday
                                Map.entry("Sun, 7 Jul 2024 20:27:52 +0200", "2024-07-07T20:27:52+02:00")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Thu, 1 Jan 1970 01:00:01 +0100"),
                                // need to check with sunday
                                Map.entry(Instant.ofEpochMilli(1720377071595L), "Sun, 7 Jul 2024 19:31:11 +0100")
                        ),
                        List.of(
                                Map.entry("03 XXX 2024 12:56:29 CET", "Failed to parse date \"03 XXX 2024 12:56:29 CET\": Invalid month name"),
                                Map.entry("03 May 2024 12:56:29  XX", "Failed to parse date \"03 May 2024 12:56:29  XX\": Unknown time-zone ID: XX")
                        )
                ),
                Arguments.of("eee, d MMM yyyy HH:mm:ss Z",
                        List.of(
                                Map.entry("Thu, 03 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                                Map.entry("03 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                                Map.entry("Thu, 3 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                                Map.entry("3 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                                Map.entry(" 3 May 2024 12:56:29 Z ", "2024-05-03T12:56:29Z")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Thu, 1 Jan 1970 01:00:01 +0100")
                        ),
                        List.of(
                                Map.entry("03 XXX 2024 12:56:29 CET", "Failed to parse date \"03 XXX 2024 12:56:29 CET\": Invalid month name"),
                                Map.entry("03 May 2024 12:56:29  XX", "Failed to parse date \"03 May 2024 12:56:29  XX\": Unknown time-zone ID: XX")
                        )
                ),
                Arguments.of("eee, dd MMM yyyy HH:mm:ss",
                        List.of(
                                Map.entry("Thu,  03 May  2024 12:56:29 ", "2024-05-03T12:56:29+01:00[Europe/London]"),
                                Map.entry(" 03 May 2024 12:56:29", "2024-05-03T12:56:29+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Thu, 01 Jan 1970 01:00:01")
                        ),
                        List.of()
                ),
                Arguments.of("dd MMM yyyy HH:mm:ss.SSS Z",
                        List.of(
                                Map.entry("Thu, 03 May 2024 12:56:29.100 Z", "2024-05-03T12:56:29.100Z"),
                                Map.entry("03 May 2024 12:56:29.1001 Z", "2024-05-03T12:56:29.100100Z")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1100), "Thu, 01 Jan 1970 01:00:01.1 +0100"),
                                Map.entry(Instant.ofEpochSecond(1, 1_100_001), "Thu, 01 Jan 1970 01:00:01.001 +0100")
                        ),
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rfc822Provider")
    void rfc822(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        runTest(pattern, toparse, toPrint, fails);
    }

    @Test
    void rfc822ImplicitYear() {
        DatetimeProcessor rfc822Processor = getProcessor("dd MMM HH:mm:ss Z");
        Assertions.assertEquals("2023-05-03T12:56:29Z", rfc822Processor.parse("Thu, 03 May 12:56:29 Z").withYear(2023).toString());
        Assertions.assertEquals("2023-05-03T12:56:29Z", rfc822Processor.parse("03 May 12:56:29 Z").withYear(2023).toString());
    }

    // -------------------------------------------------------------------------
    // rfc3164
    // -------------------------------------------------------------------------

    static Stream<Arguments> rfc3164Provider() {
        int currentYear = ZonedDateTime.now().getYear();
        return Stream.of(
                Arguments.of("rfc3164",
                        List.of(
                                Map.entry(" May 03 12:56:29", currentYear + "-05-03T12:56:29+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Jan 1 01:00:01")
                        ),
                        List.of(
                                Map.entry("XXX 03   12:56:29 +01:00", "Invalid month name"),
                                Map.entry("May 03  12:56:29 XXX", "Failed to parse date \"May 03  12:56:29 XXX\": Failed to parse date \"May 03  12:56:29 XXX\": Zone ID unexpected")
                        )
                ),
                Arguments.of("MMM d yyyy HH:mm:ss Z",
                        List.of(
                                Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry(" May 03 2024 12:56:29  Europe/Paris", "2024-05-03T12:56:29+02:00[Europe/Paris]"),
                                Map.entry("Jun 3 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Jan 1 1970 01:00:01 +0100")
                        ),
                        List.of()
                ),
                Arguments.of("MMM dd yyyy HH:mm:ss Z",
                        List.of(
                                Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry(" May 03 2024 12:56:29  CET", "2024-05-03T12:56:29+02:00[CET]"),
                                Map.entry("Jun 3 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "Jan 01 1970 01:00:01 +0100")
                        ),
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rfc3164Provider")
    void rfc3164(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        runTest(pattern, toparse, toPrint, fails);
    }

    @Test
    void rfc3164ImplicitYear() {
        DatetimeProcessor rfc3164Processor = getProcessor("MMM dd HH:mm:ss Z");
        Assertions.assertEquals("2023-05-03T12:56:29Z", rfc3164Processor.parse("May 03  12:56:29 Z").withYear(2023).toString());
        Assertions.assertEquals("2023-05-03T12:56:29Z", rfc3164Processor.parse("May 03 12:56:29 Z").withYear(2023).toString());
    }

    // -------------------------------------------------------------------------
    // timeZoneFormatingUTC
    // -------------------------------------------------------------------------

    static Stream<Arguments> timeZoneFormatingUTCProvider() {
        ZonedDateTime utc = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);
        return Stream.of(
                Arguments.of("UTC/ENGLISH", utc, Locale.ENGLISH),
                Arguments.of("UTC/FRANCE", utc, Locale.FRANCE),
                Arguments.of("UTC/CHINESE", utc, Locale.CHINESE),
                Arguments.of("Europe/Paris/ENGLISH", utc.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.ENGLISH),
                Arguments.of("Europe/Paris/FRANCE", utc.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.FRANCE),
                Arguments.of("Europe/Paris/CHINESE", utc.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.CHINESE),
                Arguments.of("+01:02:03/CHINESE", utc.withZoneSameInstant(ZoneOffset.ofHoursMinutesSeconds(1, 2, 3)), Locale.CHINESE)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timeZoneFormatingUTCProvider")
    void timeZoneFormatingUTC(String label, ZonedDateTime zdt, Locale locale) {
        formatting(zdt, locale);
    }

    private void formatting(ZonedDateTime zdt, Locale locale) {
        resolveTimeZone("MMM dd HH:mm:ss O", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss OOOO", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss VV", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss X", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss XX", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss XXX", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss XXXX", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss XXXXX", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss Z", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss ZZ", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss ZZZ", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss ZZZZ", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss ZZZZZ", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss v", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss vvvv", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss x", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss xx", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss xxx", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss xxxx", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss xxxxx", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss z", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss zz", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss zzz", zdt, locale);
        resolveTimeZone("MMM dd HH:mm:ss zzzz", zdt, locale);
    }

    private void resolveTimeZone(String pattern, ZonedDateTime toFormat, Locale locale) {
        assertInstance(pattern, DatetimeProcessorRfc3164.class);
        String tested = getProcessor(pattern).withDefaultZone(ZoneOffset.UTC).withLocale(locale).print(toFormat);
        String reference = DateTimeFormatter.ofPattern(pattern, locale).format(toFormat);
        Assertions.assertEquals(reference, tested, "Testing " + pattern);
    }

    // -------------------------------------------------------------------------
    // iso8601
    // -------------------------------------------------------------------------

    static Stream<Arguments> iso8601Provider() {
        return Stream.of(
                Arguments.of("iso",
                        List.of(
                                Map.entry("2024-05-03T12:56:29Z", "2024-05-03T12:56:29Z"),
                                Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29,1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01.001+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1970-01-01T01:00:01.001+01:00")
                        ),
                        List.of(
                                Map.entry("2024-05-03 12:56:29+01:00", "Failed to parse date \"2024-05-03 12:56:29+01:00\": Expected 'T' character but found ' '"),
                                Map.entry("2024-05-03T12:56:29XXX", "Failed to parse date \"2024-05-03T12:56:29XXX\": Unknown time-zone ID: XXX")
                        )
                ),
                Arguments.of("iso_nanos",
                        List.of(
                                Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29,1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01.001+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1970-01-01T01:00:01.000000001+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1970-01-01T01:00:01.001000001+01:00")
                        ),
                        List.of()
                ),
                Arguments.of("iso_seconds",
                        List.of(
                                Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29,1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1970-01-01T01:00:01+01:00"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1970-01-01T01:00:01+01:00")
                        ),
                        List.of()
                ),
                Arguments.of("yyyy-MM-ddXHH:mm:ss.SSZ",
                        List.of(
                                Map.entry("2024-05-03X12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                                Map.entry("2024-05-03X12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03X12:56:29,1+01:00", "2024-05-03T12:56:29.100+01:00"),
                                Map.entry("2024-05-03X12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1970-01-01X01:00:01+0100"),
                                Map.entry(Instant.ofEpochMilli(1001), "1970-01-01X01:00:01+0100"),
                                Map.entry(Instant.ofEpochMilli(1111), "1970-01-01X01:00:01.11+0100"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1970-01-01X01:00:01+0100"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1970-01-01X01:00:01+0100")
                        ),
                        List.of()
                ),
                // The default ISO 8601 format on logback
                Arguments.of("yyyy-MM-dd HH:mm:ss,SSS",
                        List.of(
                                Map.entry("2024-05-03 12:56:29", "2024-05-03T12:56:29+01:00[Europe/London]"),
                                Map.entry("2024-05-03 12:56:29.1", "2024-05-03T12:56:29.100+01:00[Europe/London]"),
                                Map.entry("2024-05-03 12:56:29,1", "2024-05-03T12:56:29.100+01:00[Europe/London]"),
                                Map.entry("2024-05-03 12:56:29,00000001", "2024-05-03T12:56:29.000000010+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1970-01-01 01:00:01"),
                                Map.entry(Instant.ofEpochMilli(1001), "1970-01-01 01:00:01,001"),
                                Map.entry(Instant.ofEpochMilli(1111), "1970-01-01 01:00:01,111"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1970-01-01 01:00:01"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1970-01-01 01:00:01,001")
                        ),
                        List.of()
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("iso8601Provider")
    void iso8601(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        runTest(pattern, toparse, toPrint, fails);
    }

    // -------------------------------------------------------------------------
    // Unix timestamps
    // -------------------------------------------------------------------------

    static Stream<Arguments> unixTimestampsProvider() {
        return Stream.of(
                Arguments.of("seconds",
                        List.of(
                                Map.entry("1", "1970-01-01T01:00:01+01:00[Europe/London]"),
                                Map.entry("1000", "1970-01-01T01:16:40+01:00[Europe/London]"),
                                Map.entry("1000000000", "2001-09-09T02:46:40+01:00[Europe/London]"),
                                Map.entry("1.000000001", "1970-01-01T01:00:01.000000001+01:00[Europe/London]"),
                                Map.entry("1.001000001", "1970-01-01T01:00:01.001000001+01:00[Europe/London]"),
                                Map.entry("1.001000001111", "1970-01-01T01:00:01.001000001+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1"),
                                Map.entry(Instant.ofEpochMilli(1001), "1.001"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1.000000001"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1.001000001")
                        ),
                        List.of(
                                Map.entry("totor", "Not a number")
                        )
                ),
                Arguments.of("milliseconds",
                        List.of(
                                Map.entry("1", "1970-01-01T01:00:00.001+01:00[Europe/London]"),
                                Map.entry("1000", "1970-01-01T01:00:01+01:00[Europe/London]"),
                                Map.entry("1000000000", "1970-01-12T14:46:40+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1000"),
                                Map.entry(Instant.ofEpochMilli(1001), "1001"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1000"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1001")
                        ),
                        List.of(
                                Map.entry("totor", "Not a number")
                        )
                ),
                Arguments.of("nanoseconds",
                        List.of(
                                Map.entry("1", "1970-01-01T01:00:00.000000001+01:00[Europe/London]"),
                                Map.entry("1000", "1970-01-01T01:00:00.000001+01:00[Europe/London]"),
                                Map.entry("1000000000", "1970-01-01T01:00:01+01:00[Europe/London]")
                        ),
                        List.of(
                                Map.entry(Instant.ofEpochMilli(1000), "1000000000"),
                                Map.entry(Instant.ofEpochMilli(1001), "1001000000"),
                                Map.entry(Instant.ofEpochSecond(1, 1), "1000000001"),
                                Map.entry(Instant.ofEpochSecond(1, 1_000_001), "1001000001")
                        ),
                        List.of(
                                Map.entry("totor", "Not a number")
                        )
                )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unixTimestampsProvider")
    void unixTimestamps(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        runTest(pattern, toparse, toPrint, fails);
    }

    @Test
    void testWithLocale() {
        DatetimeProcessor processor =  PatternResolver.createNewFormatter("rfc822")
                                                      .withLocale(Locale.FRANCE)
                                                      .withDefaultZone(ZoneId.of("Europe/London"));
        ZonedDateTime zdt = processor.parse("1 janv. 2024 04:00:00");
        Assertions.assertEquals(Month.JANUARY, zdt.getMonth());
    }

    private void assertInstance(String pattern, Class<? extends DatetimeProcessor> expected) {
        DatetimeProcessor dtp = DatetimeProcessor.of(pattern);
        Assertions.assertTrue(expected.isAssignableFrom(DatetimeProcessor.of(pattern).getClass()), String.format("Expecting %s, got %s", expected.getName(), dtp.getClass().getName()));
    }

    static Stream<Arguments> resolutionProvider() {
        return Stream.of(
                Arguments.of("iso", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd HH:mm:ss.SSZ", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd|HH:mm:ss.SSZ", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd'T'HH:mm:ss.SSZ", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd HH:mm:ss.SSS", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd HH:mm:ss,SSS", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd HH:mm:ss", DatetimeProcessorIso8601.class),
                Arguments.of("yyyy-MM-dd HH:mm:ss.SSSOOOO", DatetimeProcessorIso8601.class),
                Arguments.of("iso_nanos", DatetimeProcessorIso8601.class),
                Arguments.of("iso_seconds", DatetimeProcessorIso8601.class),
                Arguments.of("nanoseconds", DatetimeProcessorUnixNano.class),
                Arguments.of("milliseconds", DatetimeProcessorUnixMillis.class),
                Arguments.of("seconds", DatetimeProcessorUnixSeconds.class),
                Arguments.of("rfc822", DatetimeProcessorRfc822.class),
                Arguments.of("eee, d MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class),
                Arguments.of("d MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class),
                Arguments.of("dd MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class),
                Arguments.of("rfc3164", DatetimeProcessorRfc3164.class),
                Arguments.of("MMM d HH:mm:ss", DatetimeProcessorRfc3164.class),
                Arguments.of("MMM dd HH:mm:ss", DatetimeProcessorRfc3164.class),
                Arguments.of("MMM dd yyyy HH:mm:ss", DatetimeProcessorRfc3164.class),
                Arguments.of("MMM dd   yyyy HH:mm:ss", DatetimeProcessorRfc3164.class),
                Arguments.of(" MMM dd   yyyy HH:mm:ss ", DatetimeProcessorRfc3164.class),
                Arguments.of("MMM dd HH:mm:ss.SSS zzz", DatetimeProcessorRfc3164.class)
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("resolutionProvider")
    void testResolution(String pattern, Class<? extends DatetimeProcessor> expected) {
        assertInstance(pattern, expected);
    }

}
