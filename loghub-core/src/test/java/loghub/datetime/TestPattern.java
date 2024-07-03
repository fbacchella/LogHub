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

import org.junit.Assert;
import org.junit.Test;

public class TestPattern {

    private DatetimeProcessor getProcessor(String pattern) {
        return PatternResolver.createNewFormatter(pattern)
                              .withLocale(Locale.ENGLISH)
                              .withDefaultZone(ZoneId.of("Europe/London"));
    }

    private void runTest(String pattern, List<Map.Entry<String, String>> toparse, List<Map.Entry<Instant, String>> toPrint, List<Map.Entry<String, String>> fails) {
        DatetimeProcessor thisProcessor = getProcessor(pattern);
        toparse.forEach(e -> Assert.assertEquals(String.format("When parsing \"%s\"", e.getKey()), e.getValue(), thisProcessor.parse(e.getKey()).toString()));
        toPrint.forEach(e -> Assert.assertEquals(String.format("When printing \"%s\" with \"%s\"", e.getKey(), pattern), e.getValue(), thisProcessor.print(e.getKey())));
        fails.forEach(e -> {
            DateTimeParseException dtpe = Assert.assertThrows(DateTimeParseException.class, () -> thisProcessor.parse(e.getKey()));
            Assert.assertEquals(e.getValue(), dtpe.getMessage());
        });
    }

    @Test
    public void rfc822() {
        runTest("rfc822",
                List.of(
                        Map.entry("03 May 2024 12:56:29 +01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry("03 May 2024 12:56:29 CET", "2024-05-03T12:56:29+02:00[CET]"),
                        Map.entry("XXX, 03 May 2024 12:56:29 CET", "2024-05-03T12:56:29+02:00[CET]"),
                        Map.entry("Tue, 3 Jun 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Thu, 1 Jan 1970 01:00:01 +0100")
                ),
                List.of(
                        Map.entry("03 XXX 2024 12:56:29 CET", "Failed to parse date \"03 XXX 2024 12:56:29 CET\": Invalid month name"),
                        Map.entry("03 May 2024 12:56:29  XX", "Failed to parse date \"03 May 2024 12:56:29  XX\": Unknown time-zone ID: XX")
                )
        );

        runTest("eee, d MMM yyyy HH:mm:ss Z",
                List.of(
                        Map.entry("Thu, 03 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                        Map.entry("03 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                        Map.entry("Thu, 3 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                        Map.entry("3 May 2024 12:56:29 Z", "2024-05-03T12:56:29Z"),
                        Map.entry(" 3 May 2024 12:56:29 Z ", "2024-05-03T12:56:29Z")
                ), List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Thu, 1 Jan 1970 01:00:01 +0100")
                ), List.of(
                        Map.entry("03 XXX 2024 12:56:29 CET", "Failed to parse date \"03 XXX 2024 12:56:29 CET\": Invalid month name"),
                        Map.entry("03 May 2024 12:56:29  XX", "Failed to parse date \"03 May 2024 12:56:29  XX\": Unknown time-zone ID: XX")
                )
        );

        runTest("eee, dd MMM yyyy HH:mm:ss",
                List.of(
                        Map.entry("Thu,  03 May  2024 12:56:29 ", "2024-05-03T12:56:29+01:00[Europe/London]"),
                        Map.entry(" 03 May 2024 12:56:29", "2024-05-03T12:56:29+01:00[Europe/London]")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Thu, 01 Jan 1970 01:00:01")
                ),
                List.of()
        );

        runTest("dd MMM yyyy HH:mm:ss.SSS Z",
                List.of(
                        Map.entry("Thu, 03 May 2024 12:56:29.100 Z", "2024-05-03T12:56:29.100Z"),
                        Map.entry("03 May 2024 12:56:29.1001 Z", "2024-05-03T12:56:29.100100Z")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1100), "Thu, 01 Jan 1970 01:00:01.1 +0100"),
                        Map.entry(Instant.ofEpochSecond(1,1_100_001), "Thu, 01 Jan 1970 01:00:01.001 +0100")
                ),
                List.of()
        );
    }

    @Test
    public void rfc822ImplicitYear() {
        DatetimeProcessor rfc822Processor = getProcessor("dd MMM HH:mm:ss Z");
        Assert.assertEquals("2023-05-03T12:56:29Z", rfc822Processor.parse("Thu, 03 May 12:56:29 Z").withYear(2023).toString());
        Assert.assertEquals("2023-05-03T12:56:29Z", rfc822Processor.parse("03 May 12:56:29 Z").withYear(2023).toString());
    }

    @Test
    public void rfc3164() {
        int currentYear = ZonedDateTime.now().getYear();
        runTest("rfc3164",
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
        );
        runTest("MMM d yyyy HH:mm:ss Z",
                List.of(
                        Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry(" May 03 2024 12:56:29  Europe/Paris", "2024-05-03T12:56:29+02:00[Europe/Paris]"),
                        Map.entry("Jun 3 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Jan 1 1970 01:00:01 +0100")
                ),
                List.of(
                )
        );
        runTest("MMM dd yyyy HH:mm:ss Z",
                List.of(
                        Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry(" May 03 2024 12:56:29  CET", "2024-05-03T12:56:29+02:00[CET]"),
                        Map.entry("Jun 3 2008 11:05:30 +0110", "2008-06-03T11:05:30+01:10")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Jan 01 1970 01:00:01 +0100")
                ),
                List.of(
                )
        );
    }

    @Test
    public void rfc3164ImplicitYear() {
        DatetimeProcessor rfc3164Processor = getProcessor("MMM dd HH:mm:ss Z");
        Assert.assertEquals("2023-05-03T12:56:29Z", rfc3164Processor.parse("May 03  12:56:29 Z").withYear(2023).toString());
        Assert.assertEquals("2023-05-03T12:56:29Z", rfc3164Processor.parse("May 03 12:56:29 Z").withYear(2023).toString());
    }

    @Test
    public void TimeZoneFormatingUTC() {
        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(0), ZoneOffset.UTC);
        formatting(zdt, Locale.ENGLISH);
        formatting(zdt, Locale.FRANCE);
        formatting(zdt, Locale.CHINESE);
        formatting(zdt.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.ENGLISH);
        formatting(zdt.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.FRANCE);
        formatting(zdt.withZoneSameInstant(ZoneId.of("Europe/Paris")), Locale.CHINESE);
        formatting(zdt.withZoneSameInstant(ZoneOffset.ofHoursMinutesSeconds(1,2,3)), Locale.CHINESE);
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
        Assert.assertEquals("Testing " +  pattern, reference, tested);
    }

    @Test
    public void iso8164() {
        runTest("iso",
                List.of(
                        Map.entry("2024-05-03T12:56:29Z", "2024-05-03T12:56:29Z"),
                        Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                        Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01.001+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1970-01-01T01:00:01.001+01:00")
                ),
                List.of(
                        Map.entry("2024-05-03 12:56:29+01:00", "Failed to parse date \"2024-05-03 12:56:29+01:00\": Expected 'T' character but found ' '"),
                        Map.entry("2024-05-03T12:56:29XXX", "Failed to parse date \"2024-05-03T12:56:29XXX\": Unknown time-zone ID: XXX")
                )
        );
        runTest("iso_nanos",
                List.of(
                        Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                        Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01.001+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1970-01-01T01:00:01.000000001+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1970-01-01T01:00:01.001000001+01:00")
                ),
                List.of(
                )
        );
        runTest("iso_seconds",
                List.of(
                        Map.entry("2024-05-03T12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry("2024-05-03T12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                        Map.entry("2024-05-03T12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochMilli(1001), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1970-01-01T01:00:01+01:00"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1970-01-01T01:00:01+01:00")
                ),
                List.of(
                )
        );
        runTest("yyyy-MM-ddXHH:mm:ss.SSZ",
                List.of(
                        Map.entry("2024-05-03X12:56:29+01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry("2024-05-03X12:56:29.1+01:00", "2024-05-03T12:56:29.100+01:00"),
                        Map.entry("2024-05-03X12:56:29.00000001+01:00", "2024-05-03T12:56:29.000000010+01:00")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1970-01-01X01:00:01+0100"),
                        Map.entry(Instant.ofEpochMilli(1001), "1970-01-01X01:00:01+0100"),
                        Map.entry(Instant.ofEpochMilli(1111), "1970-01-01X01:00:01.11+0100"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1970-01-01X01:00:01+0100"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1970-01-01X01:00:01+0100")
                ),
                List.of(
                )
        );
    }

    @Test
    public void seconds() {
        runTest("seconds",
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
                        Map.entry(Instant.ofEpochSecond(1,1), "1.000000001"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1.001000001")
                ),
                List.of(
                        Map.entry("totor", "Not a number")
                )
        );
    }

    @Test
    public void milliseconds() {
        runTest("milliseconds",
                List.of(
                        Map.entry("1", "1970-01-01T01:00:00.001+01:00[Europe/London]"),
                        Map.entry("1000", "1970-01-01T01:00:01+01:00[Europe/London]"),
                        Map.entry("1000000000", "1970-01-12T14:46:40+01:00[Europe/London]")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1000"),
                        Map.entry(Instant.ofEpochMilli(1001), "1001"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1000"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1001")
                ),
                List.of(
                        Map.entry("totor", "Not a number")
                )
        );
    }

    @Test
    public void nanoseconds() {
        runTest("nanoseconds",
                List.of(
                        Map.entry("1", "1970-01-01T01:00:00.000000001+01:00[Europe/London]"),
                        Map.entry("1000", "1970-01-01T01:00:00.000001+01:00[Europe/London]"),
                        Map.entry("1000000000", "1970-01-01T01:00:01+01:00[Europe/London]")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "1000000000"),
                        Map.entry(Instant.ofEpochMilli(1001), "1001000000"),
                        Map.entry(Instant.ofEpochSecond(1,1), "1000000001"),
                        Map.entry(Instant.ofEpochSecond(1,1_000_001), "1001000001")
                ),
                List.of(
                        Map.entry("totor", "Not a number")
                )
        );
    }

    @Test
    public void testWithLocale() {
        DatetimeProcessor processor =  PatternResolver.createNewFormatter("rfc822")
                                                      .withLocale(Locale.FRANCE)
                                                      .withDefaultZone(ZoneId.of("Europe/London"));
        ZonedDateTime zdt = processor.parse("1 janv. 2024 04:00:00");
        Assert.assertEquals(Month.JANUARY, zdt.getMonth());
    }

    private void assertInstance(String pattern, Class<? extends DatetimeProcessor> expected) {
        DatetimeProcessor dtp = DatetimeProcessor.of(pattern);
        Assert.assertTrue(String.format("Expecting %s, got %s", expected.getName(), dtp.getClass().getName()), expected.isAssignableFrom(DatetimeProcessor.of(pattern).getClass()));
    }
    @Test
    public void testResolution() {
        assertInstance("iso", DatetimeProcessorIso8601.class);
        assertInstance("yyyy-MM-dd HH:mm:ss.SSZ", DatetimeProcessorIso8601.class);
        assertInstance("yyyy-MM-dd|HH:mm:ss.SSZ", DatetimeProcessorIso8601.class);
        assertInstance("yyyy-MM-dd'T'HH:mm:ss.SSZ", DatetimeProcessorIso8601.class);
        assertInstance("yyyy-MM-dd HH:mm:ss.SSS", DatetimeProcessorIso8601.class);
        assertInstance("iso_nanos", DatetimeProcessorIso8601.class);
        assertInstance("iso_seconds", DatetimeProcessorIso8601.class);
        assertInstance("nanoseconds", DatetimeProcessorUnixNano.class);
        assertInstance("milliseconds", DatetimeProcessorUnixMillis.class);
        assertInstance("seconds", DatetimeProcessorUnixSeconds.class);
        assertInstance("rfc822", DatetimeProcessorRfc822.class);
        assertInstance("eee, d MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class);
        assertInstance("d MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class);
        assertInstance("dd MMM yyyy HH:mm:ss Z", DatetimeProcessorRfc822.class);
        assertInstance("rfc3164", DatetimeProcessorRfc3164.class);
        assertInstance("MMM d HH:mm:ss", DatetimeProcessorRfc3164.class);
        assertInstance("MMM dd HH:mm:ss", DatetimeProcessorRfc3164.class);
        assertInstance("MMM dd yyyy HH:mm:ss", DatetimeProcessorRfc3164.class);
        assertInstance("MMM dd   yyyy HH:mm:ss", DatetimeProcessorRfc3164.class);
        assertInstance(" MMM dd   yyyy HH:mm:ss ", DatetimeProcessorRfc3164.class);
        assertInstance("MMM dd HH:mm:ss.SSS zzz", DatetimeProcessorRfc3164.class);
    }

//
//    @Test
//    public void checkZoneResolution() {
//        Set<String> usefullPatterns = new HashSet<>();
//        String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30",
//                "Pacific Time", "PT",
//                "Pacific Standard Time", "PST",
//                "GMT+8", "GMT+08:00", "UTC-08:00",
//                "Z", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "+0000", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "Europe/Paris", "CET", "CEST", "MET", "MEST",
//                "EDST", "EST", "EDT", "Eastern"
//        ).toArray(String[]::new);
//        List<String> symbols = getUsefullFormaters().keySet().stream().sorted().collect(Collectors.toList());
//        System.out.println(symbols);
//        for (String zone: zones) {
//            if (zone.isEmpty()) {
//                break;
//            }
//            System.out.print(zone);
//            ZoneId zid;
//            try {
//                zid = ZoneId.of(zone);
//                System.out.format(" = %s => %s => %s%n", zid.getId(), zid.normalized().getId(), zid.normalized().getRules());
//            } catch (Exception ex) {
//                zid = null;
//                System.out.println();
//            }
//            Set<ZoneId> resolved = new HashSet<>();
//            for (String symbol: symbols) {
//                DateTimeFormatter dtf = DateTimeFormatter.ofPattern(symbol);
//                try {
//                    ZoneId parsedZid = dtf.parse(zone, TemporalQueries.zoneId());
//                    //ZoneId parsedZid = dtf.parse(zone).query(TemporalQueries.zoneId());
//                    if (parsedZid != null) {
//                        resolved.add(parsedZid.normalized());
//                    }
//                } catch (Exception ex) {
//                }
//            }
//            System.out.println("  aka " + resolved);
//            for (String symbol: symbols) {
//                try {
//                    try {
//                        ZoneId parsedZid;
//                        String formatted = null;
//                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(symbol);
//                        try {
//                            parsedZid = dtf.parse(zone).query(TemporalQueries.zoneId());
//                        } catch (Exception ex) {
//                            parsedZid = null;
//                        }
//                        if (parsedZid != null) {
//                            formatted = dtf.format(LocalDateTime.now().atZone(parsedZid));
//                        } else if (zid != null) {
//                            formatted = dtf.format(LocalDateTime.now().atZone(zid));
//                        }
//                        if (parsedZid != null || formatted != null) {
//                            System.out.format("  %s(%s) -> %s%n", symbol, parsedZid, formatted);
//                            usefullPatterns.add(symbol);
//                        }
//                    } catch (Exception ex) {
//                        System.err.println(ex.getMessage());
//                    }
//                } catch (Exception ex) {
//                    System.err.println(ex.getMessage());
//                }
//            }
//            //System.err.format("%s -> rule=%s zid=%s parsedZid=%s[%S] formattedZid=%s%n", e.getKey(), pattern, zid, parsedZid, Optional.ofNullable(parsedZid).map(p -> p.getId().toString()).orElse(""), formatted);
//        }
//        System.err.println("Used patterns: " + usefullPatterns.stream().sorted().collect(Collectors.toList()));
//        /*String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30", // V
//                                   "Pacific Time", "PT", // v
//                                   "Pacific Standard Time", "PST", // z
//                                   "GMT+8", "GMT+08:00", "UTC-08:00" // O
//                ).toArray(String[]::new);
//        for (String zone: zones) {
//            try {
//                System.err.println(zone + " "+ ZoneId.of(zone).getRules());
//            } catch (Exception e) {
//                System.err.println(zone + " not parsed");
//            }
//        }*/
//    }
//
//    @Test
//    public void singleResolve() {
//        DateTimeFormatter.ofPattern("zzz").parse("PT");
//    }
//
//    @Test
//    public void checkByZoneString() {
//        Set<String> usefullPatterns = new HashSet<>();
//        String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30",
//                "Pacific Time", "PT",
//                "Pacific Standard Time", "PST",
//                "GMT+8", "GMT+08:00", "UTC-08:00",
//                "Z", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "+0000", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "Europe/Paris", "CET", "CEST", "MET", "MEST",
//                "EDST", "EST", "EDT", "Eastern"
//        ).toArray(String[]::new);
//        List<String> symbols = getUsefullFormaters().keySet().stream().sorted().collect(Collectors.toList());
//        System.out.println(symbols);
//        for (String zone: zones) {
//            if (zone.isEmpty()) {
//                break;
//            }
//            System.out.print(zone);
//            ZoneId zid;
//            try {
//                zid = ZoneId.of(zone);
//                System.out.format(" = %s => %s => %s%n", zid.getId(), zid.normalized().getId(), zid.normalized().getRules());
//            } catch (Exception ex) {
//                zid = null;
//                System.out.println();
//            }
//            Set<ZoneId> resolved = new HashSet<>();
//            for (String symbol: symbols) {
//                DateTimeFormatter dtf = DateTimeFormatter.ofPattern(symbol);
//                try {
//                    ZoneId parsedZid = dtf.parse(zone, TemporalQueries.zoneId());
//                    //ZoneId parsedZid = dtf.parse(zone).query(TemporalQueries.zoneId());
//                    if (parsedZid != null) {
//                        resolved.add(parsedZid.normalized());
//                    }
//                } catch (Exception ex) {
//                }
//            }
//            System.out.println("  aka " + resolved);
//            for (String symbol: symbols) {
//                try {
//                    try {
//                        ZoneId parsedZid;
//                        String formatted = null;
//                        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(symbol);
//                        try {
//                            parsedZid = dtf.parse(zone).query(TemporalQueries.zoneId());
//                        } catch (Exception ex) {
//                            parsedZid = null;
//                        }
//                        if (parsedZid != null) {
//                            formatted = dtf.format(LocalDateTime.now().atZone(parsedZid));
//                        } else if (zid != null) {
//                            formatted = dtf.format(LocalDateTime.now().atZone(zid));
//                        }
//                        if (parsedZid != null || formatted != null) {
//                            System.out.format("  %s(%s) -> %s%n", symbol, parsedZid, formatted);
//                            usefullPatterns.add(symbol);
//                        }
//                    } catch (Exception ex) {
//                        System.err.println(ex.getMessage());
//                    }
//                } catch (Exception ex) {
//                    System.err.println(ex.getMessage());
//                }
//            }
//            //System.err.format("%s -> rule=%s zid=%s parsedZid=%s[%S] formattedZid=%s%n", e.getKey(), pattern, zid, parsedZid, Optional.ofNullable(parsedZid).map(p -> p.getId().toString()).orElse(""), formatted);
//        }
//        System.err.println("Used patterns: " + usefullPatterns.stream().sorted().collect(Collectors.toList()));
//        /*String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30", // V
//                                   "Pacific Time", "PT", // v
//                                   "Pacific Standard Time", "PST", // z
//                                   "GMT+8", "GMT+08:00", "UTC-08:00" // O
//                ).toArray(String[]::new);
//        for (String zone: zones) {
//            try {
//                System.err.println(zone + " "+ ZoneId.of(zone).getRules());
//            } catch (Exception e) {
//                System.err.println(zone + " not parsed");
//            }
//        }*/
//    }
//
//    @Test
//    public void testFormatters() {
//        String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30",
//                "Pacific Time", "PT",
//                "Pacific Standard Time", "PST",
//                "GMT+8", "GMT+08:00", "UTC-08:00",
//                "Z", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "+0000", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "Europe/Paris", "CET", "CEST", "MET", "MEST",
//                "EDST", "EST", "EDT", "Eastern"
//        ).toArray(String[]::new);
//        Map<String, DateTimeFormatter> formatters = getUsefullFormaters();
//        for (String zone: zones) {
//            System.out.println(zone + ": ");
//            Optional.of(TimeZone.getTimeZone(zone))
//                    .filter(tz -> ! "GMT".equals(tz.getID()))
//                    .ifPresent(z -> System.out.format("  TimeZone => %s = %s%n", z.toZoneId(), z.toZoneId().normalized().getRules()));
//            try {
//                ZoneId zid = ZoneId.of(zone);
//                System.out.format("  ZoneId => %s%n", zid.normalized().getRules());
//            } catch (Exception ex) {
//                // pass
//            }
//            for (Map.Entry<String, DateTimeFormatter> e:  formatters.entrySet()) {
//                resolveZone(zone, e.getValue()).ifPresent(z -> System.out.format("  %s => %s%n", e.getKey(), z.normalized().getRules()));
//            }
//
//        }
//    }
//
//    @Test
//    public void testParse() {
//        String[] zones = Stream.of("America/Los_Angeles", "Z", "-08:30",
//                "Pacific Time", "PT",
//                "Pacific Standard Time", "PST",
//                "GMT+8", "GMT+08:00", "UTC-08:00",
//                "Z", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "+0000", "-08", "-0830", "-08:30", "-083015", "-08:30:15",
//                "Europe/Paris", "CET", "CEST", "MET", "MEST",
//                "EDST", "EST", "EDT", "Eastern"
//        ).toArray(String[]::new);
//        System.out.println("TimeZone: ");
//        for (String zone: zones) {
//            try {
//                TimeZone zn = TimeZone.getTimeZone(zone);
//                System.out.format("  %s => %s%n", zone, zn.toZoneId());
//            } catch (Exception e) {
//                System.out.format("  %s => %s%n", zone, e.getMessage());
//            }
//        }
//        System.out.println("ZoneOffset: ");
//        for (String zone: zones) {
//            try {
//                ZoneId zid = ZoneOffset.of(zone);
//                System.out.format("  %s => %s%n", zone, zid.normalized());
//            } catch (Exception e) {
//                System.out.format("  %s => %s%n", zone, e.getMessage());
//            }
//        }
//        System.out.println("ZoneId: ");
//        for (String zone: zones) {
//            try {
//                ZoneId zid = ZoneId.of(zone);
//                System.out.format("  %s => %s%n", zone, zid.normalized());
//            } catch (Exception e) {
//                System.out.format("  %s => %s%n", zone, e.getMessage());
//            }
//        }
//
//        for (Map.Entry<String, DateTimeFormatter> e:  getUsefullFormaters().entrySet()) {
//            System.out.println(e.getKey() + ": ");
//            for (String zone: zones) {
//                resolveZone(zone, e.getValue()).ifPresent(z -> System.out.format("  %s => %s%n", zone, z.normalized().getId()));
//            }
//        }
//        System.out.println("ParsingContext: ");
//        for (String zone: zones) {
//            try {
//                ParsingContext ctx = new ParsingContext(zone);
//                ZoneId zid = ctx.extractOffset((sb, zdt) -> sb, ZoneId.systemDefault());
//                System.out.format("  %s => %s%n", zone, zid.normalized());
//            } catch (Exception e) {
//                System.out.format("  %s => %s%n", zone, e.getMessage());
//            }
//
//            ParsingContext ctx = new ParsingContext(zone);
//        }
//    }
//
//    Optional<ZoneId> resolveZone(String zone, DateTimeFormatter dtf) {
//        try {
//            return Optional.of(dtf.parse(zone).query(TemporalQueries.zoneId()));
//        } catch (Exception ex) {
//            return Optional.empty();
//        }
//    }
//
//    private Map<String, DateTimeFormatter> getUsefullFormaters() {
//        Map<String, DateTimeFormatter> formatters = new HashMap<>();
//        for (String trySymbol: Stream.of("V", "v", "z", "O", "X", "x", "Z").toArray(String[]::new)) {
//            String tryPattern = "";
//            for (int i = 0 ; i < 5 ; i++) {
//                tryPattern += trySymbol;
//                try {
//                    formatters.put(tryPattern, DateTimeFormatter.ofPattern(tryPattern));
//                } catch (Exception e) {
//                    //System.err.println(tryPattern + " " + e.getMessage());
//                }
//            }
//        }
//        return Map.copyOf(formatters);
//    }

//    @Test
//    public void zrp() {
//        /*ServiceLoader.load(TimeZoneNameProvider.class).forEach(tznp ->
//                System.out.println(tznp)
//        );*/
//        /*for (String s: ZoneRulesProvider.getAvailableZoneIds()) {
//            ZoneRules zr = ZoneRulesProvider.getRules(s, true);
//            System.err.println(zr);
//        }*/
//        for(String[] zones: DateFormatSymbols.getInstance(Locale.FRANCE).getZoneStrings()) {
//            System.out.println(Arrays.toString(zones));
//            //System.out.println(TimeZone.getTimeZone(zones[2]).toZoneId().normalized());
//            //System.out.println(ZoneId
//        }
//    }
}
