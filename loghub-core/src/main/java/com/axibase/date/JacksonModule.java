package com.axibase.date;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class JacksonModule extends SimpleModule {

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final DatetimeProcessor AS_IS8601_NANO = PatternResolver.createNewFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final DatetimeProcessor AS_IS8601_NANO_WITH_ZONEID = PatternResolver.createNewFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSZZ'['VV']'");
    private static final DatetimeProcessor AS_IS8601_MILLI = PatternResolver.createNewFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSxxx");
    private static final BigDecimal GIGA = BigDecimal.valueOf(1_000_000_000L);
    private static final int WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS = 1;
    private static final int WRITE_DATES_AS_TIMESTAMPS = 2;
    private static final int WRITE_DATES_WITH_CONTEXT_TIME_ZONE = 4;
    private static final int WRITE_DATES_WITH_ZONE_ID = 8;

    private static final Map<SerializationFeature, Integer> FEATURES_VALUE = Map.of(
            SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS,
            SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, WRITE_DATES_AS_TIMESTAMPS,
            SerializationFeature.WRITE_DATES_WITH_CONTEXT_TIME_ZONE, WRITE_DATES_WITH_CONTEXT_TIME_ZONE,
            SerializationFeature.WRITE_DATES_WITH_ZONE_ID, WRITE_DATES_WITH_ZONE_ID
    );

    private abstract static class DateTimeSerializer<T> extends StdSerializer<T> {
        protected DateTimeSerializer(Class<T> t) {
            super(t);
        }

        protected void resolve(JsonGenerator gen, SerializerProvider provider,
                boolean withNanoPrecision,
                BiFunction<ZoneId, DatetimeProcessor, String> asString, Function<Boolean, BigDecimal> asNumber)
                throws IOException {
            SerializationConfig config = provider.getConfig();
            int activeFeatures = FEATURES_VALUE.keySet()
                                               .stream()
                                               .filter(config::isEnabled)
                                               .mapToInt(FEATURES_VALUE::get)
                                               .reduce(0, Integer::sum);
            boolean wantNano = withNanoPrecision && (activeFeatures & WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS) > 0;

            DatetimeProcessor provided = Optional.ofNullable(provider.getConfig().getDateFormat())
                                                 .filter(DateFormatWrapper.class::isInstance)
                                                 .map(df -> ((DateFormatWrapper)df).getProcessor())
                                                 .orElseGet(() -> resolve(withNanoPrecision, activeFeatures));
            provided = provided.withLocale(provider.getLocale()).withDefaultZone(provider.getTimeZone().toZoneId());
            ZoneId ctxZid = (activeFeatures & WRITE_DATES_WITH_CONTEXT_TIME_ZONE) > 0 ?
                                    provider.getTimeZone().toZoneId():
                                    null;
            if ((activeFeatures & WRITE_DATES_AS_TIMESTAMPS) > 0) {
                gen.writeNumber(asNumber.apply(wantNano));
            } else {
                gen.writeString(asString.apply(ctxZid, provided));
            }
        }

        private DatetimeProcessor resolve(boolean nanoPrecision, int activeFeatures) {
            if ((activeFeatures & WRITE_DATES_WITH_ZONE_ID) > 0) {
                return nanoPrecision ? AS_IS8601_NANO_WITH_ZONEID : AS_IS8601_MILLI;
            } else {
                return nanoPrecision ? AS_IS8601_NANO : AS_IS8601_MILLI;
            }
        }

        private BigDecimal asNanoseconds(long seconds, int nanoseconds) {
            return BigDecimal.valueOf(nanoseconds).divide(GIGA, MathContext.UNLIMITED).add(BigDecimal.valueOf(seconds));
        }

        BigDecimal asNanoseconds(boolean wantNano, Instant value) {
            return wantNano ?
                           asNanoseconds(value.getEpochSecond(), value.getNano()) :
                           new BigDecimal(value.toEpochMilli());
        }
    }

    public static class InstantSerializer extends DateTimeSerializer<Instant> {
        public InstantSerializer() {
            super(Instant.class);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> AS_IS8601_NANO.print(value.atZone(UTC)),
                    u -> asNanoseconds(u, value)
            );
        }
    }
    public static class ZonedDateTimeSerializer extends DateTimeSerializer<ZonedDateTime> {
        public ZonedDateTimeSerializer() {
            super(ZonedDateTime.class);
        }

        @Override
        public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> checkProvidedTimeZone(z, value, dtp),
                    u -> asNanoseconds(u, value.toInstant()));
        }
        private String checkProvidedTimeZone(ZoneId zid, ZonedDateTime value, DatetimeProcessor dtp) {
            if (zid == null) {
                return dtp.print(value);
            } else {
                ZonedDateTime newValue = value.withZoneSameInstant(zid);
                return dtp.print(newValue);
            }
        }
    }

    public static class DateSerializer extends DateTimeSerializer<Date> {
        public DateSerializer() {
            super(Date.class);
        }
        @Override
        public void serialize(Date value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    false,
                    (z, dtp) -> AS_IS8601_MILLI.print(value.getTime(), provider.getTimeZone().toZoneId()),
                    u -> asNanoseconds(u, value.toInstant()));
        }
    }

    public static class CalendarSerializer extends DateTimeSerializer<Calendar> {
        public CalendarSerializer() {
            super(Calendar.class);
        }
        @Override
        public void serialize(Calendar value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    false,
                    (z, dtp) -> AS_IS8601_MILLI.print(value.getTimeInMillis(), provider.getTimeZone().toZoneId()),
                    u -> asNanoseconds(u, value.toInstant()));
        }
    }

    @Override
    public void setupModule(SetupContext context) {
        super.setupModule(context);
        SimpleSerializers sers = new SimpleSerializers();
        sers.addSerializer(Instant.class, new InstantSerializer());
        sers.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
        sers.addSerializer(Date.class, new DateSerializer());
        sers.addSerializer(Calendar.class, new CalendarSerializer());
        context.addSerializers(sers);
    }
}
