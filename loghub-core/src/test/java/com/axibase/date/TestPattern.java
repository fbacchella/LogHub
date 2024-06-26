package com.axibase.date;

import java.time.Instant;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
                        Map.entry("03 XXX 2024 12:56:29 CET", "Invalid month name"),
                        Map.entry("03 May 2024 12:56:29  XX", "Unknown time-zone ID: XX")
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
                        Map.entry("03 XXX 2024 12:56:29 CET", "Invalid month name"),
                        Map.entry("03 May 2024 12:56:29  XX", "Unknown time-zone ID: XX")
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
                        Map.entry(" May 03 12:56:29  +01:00", currentYear + "-05-03T12:56:29+01:00"),
                        Map.entry(" May 03 12:56:29  CET", currentYear + "-05-03T12:56:29+02:00[CET]")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Jan 1 01:00:01 +0100")
                ),
                List.of(
                        Map.entry("XXX 03   12:56:29 +01:00", "Invalid month name"),
                        Map.entry("May 03  12:56:29 XXX", "Unknown time-zone ID: XXX")
                )
        );
        runTest("MMM d yyyy HH:mm:ss Z",
                List.of(
                        Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry(" May 03 2024 12:56:29  CET", "2024-05-03T12:56:29+02:00[CET]"),
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
                        Map.entry("2024-05-03 12:56:29+01:00", "Failed to parse date 2024-05-03 12:56:29+01:00: Expected 'T' character but found ' '"),
                        Map.entry("2024-05-03T12:56:29XXX", "Failed to parse date 2024-05-03T12:56:29XXX: Invalid ID for ZoneOffset, non numeric characters found: XXX")
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
                        Map.entry("1000000000", "2001-09-09T02:46:40+01:00[Europe/London]")
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

}
