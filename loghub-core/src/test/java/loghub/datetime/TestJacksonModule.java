package loghub.datetime;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import loghub.LogUtils;
import loghub.Tools;

public class TestJacksonModule {

    private static Logger logger ;

    @BeforeClass
    static public void configure() {
        Tools.configure();
        logger = LogManager.getLogger();
        LogUtils.setLevel(logger, Level.DEBUG);
    }

    private static TimeZone defaultTz;
    @BeforeClass
    public static void saveTimeZone() {
        defaultTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Tokyo")));
    }
    @AfterClass
    public static void restoreTimeZone() {
        TimeZone.setDefault(defaultTz);
    }

    public JsonMapper getMapper(Consumer<JsonMapper.Builder> configurator) {
        JsonMapper.Builder builder = JsonMapper.builder();
        builder.setDefaultTyping(StdTypeResolverBuilder.noTypeInfoBuilder());
        builder.addModule(new JavaTimeModule());
        builder.addModule(new Jdk8Module());
        builder.addModule(new AfterburnerModule());
        configurator.accept(builder);
        return builder.build();
    }

    public void runTest(Object value, Map.Entry<String, Consumer<JsonMapper>> mapperConfigurator, String expected) {
        try {
            JsonMapper simpleMapper = getMapper(m -> {});
            JsonMapper axibaseMapper = getMapper(m -> m.addModule(new JacksonModule()));
            mapperConfigurator.getValue().accept(simpleMapper);
            mapperConfigurator.getValue().accept(axibaseMapper);
            ObjectWriter axibaseWritter =  axibaseMapper.writerFor(Object.class);
            logger.debug("  {} {}", value.getClass().getName(),axibaseWritter.writeValueAsString(value));
            Assert.assertEquals(value.getClass().getName(), expected, axibaseWritter.writeValueAsString(value));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public void check(Object value, List<Map.Entry<String, Consumer<JsonMapper>>> configurators, List<String> expected) {
        Assert.assertEquals(configurators.size(), expected.size());
        for (int i = 0; i < configurators.size(); i++) {
            runTest(value, configurators.get(i), expected.get(i));
        }
    }

    private Calendar fromInstant(Instant i, ZoneId tz) {
        return new Calendar.Builder().setCalendarType("iso8601").setInstant(i.toEpochMilli()).setTimeZone(TimeZone.getTimeZone(tz)).build();
    }

    @Test
    public void testDefaultSettigs() {
        List<Map.Entry<String, Consumer<JsonMapper>>> configurators = List.of(
                Map.entry("TIMESTAMPS_AS_MILLISECONDS",
                      m -> m.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS",
                    m -> m.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)),
                Map.entry("AS_STRING",
                        m -> {
                        m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                        m.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
                        m.disable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
                    }
                ),
                Map.entry("WITH_ZONE_ID",
                    m -> {
                        m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                        m.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
                        m.disable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
                    }
                ),
                Map.entry("WITH_CONTEXT_TIME_ZONE",
                    m -> {
                        m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                        m.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
                        m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
                    }
                ),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID",
                    m -> {
                        m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
                        m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
                        m.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
                        m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
                    }
                )
        );
        ZoneId moscow = ZoneId.of("Europe/Moscow");
        long seconds = 1714421498L;
        long withNanos = 936_155_001L;
        long withMilli = 936_000_000L;
        long withDeci = 900_000_000L;
        long withoutSubSecond = 0L;
        List<Map.Entry<String, List<Object>>> values = List.of(
                Map.entry("Instant with nano",
                        List.of(Instant.ofEpochSecond(seconds, withNanos))
                ),
                Map.entry("Instant with milli",
                        List.of(
                            Instant.ofEpochSecond(seconds, withMilli),
                            Date.from(Instant.ofEpochSecond(seconds, withMilli))
                        )
                ),
                Map.entry("Instant with deci",
                        List.of(
                            Instant.ofEpochSecond(seconds, withDeci),
                            Date.from(Instant.ofEpochSecond(seconds, withDeci))
                        )
                ),
                Map.entry("Instant without subseconds",
                        List.of(
                            Instant.ofEpochSecond(seconds, withoutSubSecond),
                            Date.from(Instant.ofEpochSecond(seconds, withoutSubSecond))
                        )
                ),
                Map.entry("Date with nano",
                        List.of(
                            Instant.ofEpochSecond(seconds, withNanos).atZone(moscow)
                        )
                ),
                Map.entry("Date with milli",
                        List.of(
                            Instant.ofEpochSecond(seconds, withMilli).atZone(moscow),
                            fromInstant(Instant.ofEpochSecond(seconds, withMilli), moscow)
                        )
                ),
                Map.entry("Date without subseconds",
                        List.of(
                            Instant.ofEpochSecond(seconds, withoutSubSecond).atZone(moscow),
                            fromInstant(Instant.ofEpochSecond(seconds, withoutSubSecond), moscow)
                        )
                ),
                Map.entry("DateOffset with nano",
                        List.of(
                            OffsetDateTime.ofInstant(Instant.ofEpochSecond(seconds, withNanos), moscow)
                        )
                ),
                Map.entry("LocalDate with nano",
                        List.of(
                            LocalDateTime.ofInstant(Instant.ofEpochSecond(seconds, withNanos), moscow)
                        )
                )
        );
        Map<String, String> expected = Map.ofEntries(
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Instant with nano", "1714421498936"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Instant with milli", "1714421498936"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Instant with deci", "1714421498900"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Instant without subseconds", "1714421498000"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Date with nano", "1714421498936"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Date with milli", "1714421498936"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/Date without subseconds", "1714421498000"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/DateOffset with nano", "1714421498936"),
                Map.entry("TIMESTAMPS_AS_MILLISECONDS/LocalDate with nano", "1714399898936"),

                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Instant with nano", "1714421498.936155001"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Instant with milli", "1714421498.936"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Instant with deci", "1714421498.9"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Instant without subseconds", "1714421498"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Date with nano", "1714421498.936155001"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Date with milli", "1714421498.936"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/Date without subseconds", "1714421498"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/DateOffset with nano", "1714421498.936155001"),
                Map.entry("TIMESTAMPS_AS_NANOSECONDS/LocalDate with nano", "1714399898.936155001"),

                Map.entry("AS_STRING/Instant with nano", "\"2024-04-29T20:11:38.936155001Z\""),
                Map.entry("AS_STRING/Instant with milli", "\"2024-04-29T20:11:38.936Z\""),
                Map.entry("AS_STRING/Instant with deci", "\"2024-04-29T20:11:38.9Z\""),
                Map.entry("AS_STRING/Instant without subseconds", "\"2024-04-29T20:11:38Z\""),
                Map.entry("AS_STRING/Date with nano", "\"2024-04-29T23:11:38.936155001+03:00\""),
                Map.entry("AS_STRING/Date with milli", "\"2024-04-29T23:11:38.936+03:00\""),
                Map.entry("AS_STRING/Date without subseconds", "\"2024-04-29T23:11:38+03:00\""),
                Map.entry("AS_STRING/DateOffset with nano", "\"2024-04-29T23:11:38.936155001+03:00\""),
                Map.entry("AS_STRING/LocalDate with nano", "\"2024-04-29T23:11:38.936155001+09:00\""),

                Map.entry("WITH_ZONE_ID/Instant with nano", "\"2024-04-29T20:11:38.936155001Z[UTC]\""),
                Map.entry("WITH_ZONE_ID/Instant with milli", "\"2024-04-29T20:11:38.936Z[UTC]\""),
                Map.entry("WITH_ZONE_ID/Instant with deci", "\"2024-04-29T20:11:38.9Z[UTC]\""),
                Map.entry("WITH_ZONE_ID/Instant without subseconds", "\"2024-04-29T20:11:38Z[UTC]\""),
                Map.entry("WITH_ZONE_ID/Date with nano", "\"2024-04-29T23:11:38.936155001+03:00[Europe/Moscow]\""),
                Map.entry("WITH_ZONE_ID/Date with milli", "\"2024-04-29T23:11:38.936+03:00[Europe/Moscow]\""),
                Map.entry("WITH_ZONE_ID/Date without subseconds", "\"2024-04-29T23:11:38+03:00[Europe/Moscow]\""),
                Map.entry("WITH_ZONE_ID/DateOffset with nano", "\"2024-04-29T23:11:38.936155001+03:00[+03:00]\""),
                Map.entry("WITH_ZONE_ID/LocalDate with nano", "\"2024-04-29T23:11:38.936155001+09:00[Asia/Tokyo]\""),

                Map.entry("WITH_CONTEXT_TIME_ZONE/Instant with nano", "\"2024-04-29T16:11:38.936155001-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Instant with milli", "\"2024-04-29T16:11:38.936-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Instant with deci", "\"2024-04-29T16:11:38.9-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Instant without subseconds", "\"2024-04-29T16:11:38-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Date with nano", "\"2024-04-29T16:11:38.936155001-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Date with milli", "\"2024-04-29T16:11:38.936-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/Date without subseconds", "\"2024-04-29T16:11:38-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/DateOffset with nano", "\"2024-04-29T16:11:38.936155001-04:00\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE/LocalDate with nano", "\"2024-04-29T10:11:38.936155001-04:00\""),

                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Instant with nano", "\"2024-04-29T16:11:38.936155001-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Instant with milli", "\"2024-04-29T16:11:38.936-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Instant with deci", "\"2024-04-29T16:11:38.9-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Instant without subseconds", "\"2024-04-29T16:11:38-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Date with nano", "\"2024-04-29T16:11:38.936155001-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Date with milli", "\"2024-04-29T16:11:38.936-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/Date without subseconds", "\"2024-04-29T16:11:38-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/DateOffset with nano", "\"2024-04-29T16:11:38.936155001-04:00[America/New_York]\""),
                Map.entry("WITH_CONTEXT_TIME_ZONE_AND_ID/LocalDate with nano", "\"2024-04-29T10:11:38.936155001-04:00[America/New_York]\"")
        );
        for (Map.Entry<String, Consumer<JsonMapper>> c: configurators) {
            for (Map.Entry<String, List<Object>> v: values) {
                logger.debug("{}/{}", c.getKey(), v.getKey());
                String expectedValue = expected.get(String.format("%s/%s", c.getKey(), v.getKey()));
                for (Object o: v.getValue()) {
                    runTest(o, c, expectedValue);
                }
            }
        }
    }

}
