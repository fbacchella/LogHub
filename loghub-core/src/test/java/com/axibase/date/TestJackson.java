package com.axibase.date;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;

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
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

public class TestJackson {
    private static TimeZone defaultTz;
    @BeforeClass
    public static void saveTimeZone() {
        defaultTz = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Japan/Tokyo"));
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

    public void runTest(Object value, Consumer<JsonMapper> mapperConfigurator) {
        try {
            JsonMapper simpleMapper = getMapper(m -> {});
            JsonMapper axibaseMapper = getMapper(m -> m.addModule(new JacksonModule()));
            mapperConfigurator.accept(simpleMapper);
            mapperConfigurator.accept(axibaseMapper);
            ObjectWriter simpleWritter =  simpleMapper.writerFor(Object.class);
            ObjectWriter axibaseWritter =  axibaseMapper.writerFor(Object.class);
            Map<String, Object> mapdata = Map.of("now", value);
            System.err.format("%s %s == %s%n", value.getClass().getName(), simpleWritter.writeValueAsString(mapdata), axibaseWritter.writeValueAsString(mapdata));
            //Assert.assertEquals(simpleWritter.writeValueAsString(mapdata), axibaseWritter.writeValueAsString(mapdata));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testDefaultSettigs() {
        List<Consumer<JsonMapper>> configurators = new ArrayList<>(5);
        configurators.add(m -> m.disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS));
        configurators.add(m -> m.enable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS));
        configurators.add(m -> {
            m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            m.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
            m.disable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
        });
        configurators.add(m -> {
            m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            m.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
            m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
        });
        configurators.add(m -> {
            m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            m.disable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
            m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
        });
        configurators.add(m -> {
            m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            m.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
            m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
        });
        ZoneId moscow = ZoneId.of("Europe/Moscow");

        configurators.forEach(c -> runTest(Instant.now().plusNanos(1), c));

        ZonedDateTime nowZDT = Instant.now().plusNanos(1).atZone(moscow);
        configurators.forEach(c -> runTest(nowZDT, c));

        configurators.forEach(c -> runTest(Date.from(Instant.now()), c));

        Calendar nowCalender = Calendar.getInstance(TimeZone.getTimeZone(moscow));
        nowCalender.setTimeInMillis(Instant.now().toEpochMilli());
        configurators.forEach(c -> runTest(nowCalender, c));
    }

    @Test
    public void failing() {
        Consumer<JsonMapper> configurator = m -> {
            m.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            m.enable(SerializationFeature.WRITE_DATES_WITH_ZONE_ID);
            m.enable(SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE);
        };
        ZoneId moscow = ZoneId.of("Europe/Moscow");
        ZonedDateTime value = Instant.now().plusNanos(1).atZone(moscow);
        runTest(value, configurator);
    }
}
