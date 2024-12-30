package loghub.datetime;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import loghub.VarFormatter;

import static java.time.ZoneOffset.UTC;

public class JacksonModule extends SimpleModule {

    private static final DatetimeProcessor AS_IS8601_NANO = PatternResolver.createNewFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
    private static final DatetimeProcessor AS_IS8601_MILLI = PatternResolver.createNewFormatter("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    private static final BigDecimal GIGA = BigDecimal.valueOf(1_000_000_000L);
    private static final BigDecimal KILO = BigDecimal.valueOf(1_000L);
    private static final VarFormatter ZONE_ID_FORMAT = new VarFormatter("${#1%s}[${#2%s}]");
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
        private final Map<SerializationConfig, Integer> configurationCache;

        protected DateTimeSerializer(Map<SerializationConfig, Integer> configurationCache, Class<T> t) {
            super(t);
            this.configurationCache = configurationCache;
        }

        protected void resolve(JsonGenerator gen, SerializerProvider provider,
                boolean withNanoPrecision,
                BiFunction<ZoneId, DatetimeProcessor, String> asString, Function<Boolean, BigDecimal> asNumber,
                Function<ZoneId, String> getZoneId
        ) throws IOException {
            int activeFeatures = configurationCache.computeIfAbsent(provider.getConfig(), this::getFeatures);

            DatetimeProcessor iso8601Processor = withNanoPrecision ? AS_IS8601_NANO : AS_IS8601_MILLI;
            iso8601Processor = iso8601Processor.withLocale(provider.getLocale()).withDefaultZone(provider.getTimeZone().toZoneId());

            ZoneId ctxZid = (activeFeatures & WRITE_DATES_WITH_CONTEXT_TIME_ZONE) > 0 ?
                                    provider.getTimeZone().toZoneId() :
                                    null;

            if ((activeFeatures & WRITE_DATES_AS_TIMESTAMPS) > 0) {
                gen.writeNumber(asNumber.apply((activeFeatures & WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS) > 0));
            } else {
                if ((activeFeatures & WRITE_DATES_WITH_ZONE_ID) > 0) {
                    String zoneIdentifier = getZoneId.apply(ctxZid);
                    String formatted = asString.apply(ctxZid, iso8601Processor);
                    if (zoneIdentifier.isEmpty()) {
                        gen.writeString(formatted);
                    } else {
                        gen.writeString(ZONE_ID_FORMAT.argsFormat(formatted, zoneIdentifier));
                    }
                } else {
                    gen.writeString(asString.apply(ctxZid, iso8601Processor));
                }
            }
        }

        private BigDecimal asNanoseconds(long seconds, int nanoseconds) {
            return new BigDecimal(nanoseconds).divide(GIGA, MathContext.UNLIMITED).add(BigDecimal.valueOf(seconds));
        }

        BigDecimal asNanoseconds(boolean wantNano, long milliseconds) {
            return wantNano ?
                 new BigDecimal(milliseconds).divide(KILO, MathContext.UNLIMITED) :
                 new BigDecimal(milliseconds);
        }

        BigDecimal asNanoseconds(boolean wantNano, Instant value) {
            return wantNano ?
                           asNanoseconds(value.getEpochSecond(), value.getNano()) :
                           new BigDecimal(value.toEpochMilli());
        }

        private Integer getFeatures(SerializationConfig config) {
            return FEATURES_VALUE.keySet()
                                 .stream()
                                 .filter(config::isEnabled)
                                 .mapToInt(FEATURES_VALUE::get)
                                 .reduce(0, Integer::sum);
        }
    }

    public static class InstantSerializer extends DateTimeSerializer<Instant> {
        public InstantSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, Instant.class);
        }

        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> AS_IS8601_NANO.print(value.atZone(z != null ? z : UTC)),
                    u -> asNanoseconds(u, value),
                    z -> (z != null ? z.getId() : "UTC")
            );
        }
    }

    public static class ZonedDateTimeSerializer extends DateTimeSerializer<ZonedDateTime> {
        public ZonedDateTimeSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, ZonedDateTime.class);
        }

        @Override
        public void serialize(ZonedDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> checkProvidedTimeZone(z, value, dtp),
                    u -> asNanoseconds(u, value.toInstant()),
                    z -> (z != null ? z : value.getZone()).getId()
            );
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

    public static class OffsetDateTimeSerializer extends DateTimeSerializer<OffsetDateTime> {
        public OffsetDateTimeSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, OffsetDateTime.class);
        }

        @Override
        public void serialize(OffsetDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> checkProvidedTimeZone(z, value, dtp),
                    u -> asNanoseconds(u, value.toInstant()),
                    z -> (z != null ? z : value.toZonedDateTime().getZone()).getId()
            );
        }
        private String checkProvidedTimeZone(ZoneId zid, OffsetDateTime value, DatetimeProcessor dtp) {
            if (zid == null) {
                return dtp.print(value.toZonedDateTime());
            } else {
                ZonedDateTime newValue = value.toZonedDateTime().withZoneSameInstant(zid);
                return dtp.print(newValue);
            }
        }
    }

    public static class LocalDateTimeSerializer extends DateTimeSerializer<LocalDateTime> {
        public LocalDateTimeSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, LocalDateTime.class);
        }

        @Override
        public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    true,
                    (z, dtp) -> checkProvidedTimeZone(z, value.atZone(ZoneId.systemDefault()), dtp),
                    u -> asNanoseconds(u, value.atZone(ZoneId.systemDefault()).toInstant()),
                    z -> (z != null ? z : ZoneId.systemDefault()).getId()
            );
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
        public DateSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, Date.class);
        }
        @Override
        public void serialize(Date value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    false,
                    (z, dtp) -> AS_IS8601_MILLI.withDefaultZone(z != null ? z : UTC).print(value.toInstant()),
                    u -> asNanoseconds(u, value.getTime()),
                    z -> (z != null ? z.getId() : "UTC")
            );
        }
    }

    public static class CalendarSerializer extends DateTimeSerializer<Calendar> {
        public CalendarSerializer(Map<SerializationConfig, Integer> configurationCache) {
            super(configurationCache, Calendar.class);
        }
        @Override
        public void serialize(Calendar value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            resolve(gen, provider,
                    false,
                    (z, dtp) -> AS_IS8601_MILLI.withDefaultZone(z != null ? z : value.getTimeZone().toZoneId()).print(value.toInstant()),
                    u -> asNanoseconds(u, value.getTimeInMillis()),
                    z -> (z != null ? z : value.getTimeZone().toZoneId()).getId()
            );
        }
    }

    @Override
    public void setupModule(SetupContext context) {
        Map<SerializationConfig, Integer> configurationCache = new ConcurrentHashMap<>();
        super.setupModule(context);
        SimpleSerializers sers = new SimpleSerializers();
        sers.addSerializer(Instant.class, new InstantSerializer(configurationCache));
        sers.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer(configurationCache));
        sers.addSerializer(OffsetDateTime.class, new OffsetDateTimeSerializer(configurationCache));
        sers.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(configurationCache));
        sers.addSerializer(Date.class, new DateSerializer(configurationCache));
        sers.addSerializer(Calendar.class, new CalendarSerializer(configurationCache));
        context.addSerializers(sers);
    }

}
