package com.axibase.date;

import java.time.Instant;
import java.time.ZoneId;
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
                        Map.entry("XXX, 03 May 2024 12:56:29 CET", "2024-05-03T12:56:29+02:00[CET]")
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
        runTest("rfc3164",
                List.of(
                        Map.entry(" May 03 2024 12:56:29  +01:00", "2024-05-03T12:56:29+01:00"),
                        Map.entry(" May 03 2024 12:56:29  CET", "2024-05-03T12:56:29+02:00[CET]")
                ),
                List.of(
                        Map.entry(Instant.ofEpochMilli(1000), "Jan 01 1970 01:00:01 +0100")
                ),
                List.of(
                        Map.entry("XXX 03 2024  12:56:29 +01:00", "Invalid month name"),
                        Map.entry("May 03 2024  12:56:29 XXX", "Unknown time-zone ID: XXX")
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
    }

}
