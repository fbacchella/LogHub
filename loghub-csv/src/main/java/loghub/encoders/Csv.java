package loghub.encoders;

import java.nio.charset.Charset;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import loghub.datetime.DatetimeProcessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.CanBatch;
import loghub.Expression;
import loghub.IgnoredEventException;
import loghub.ProcessorException;
import loghub.events.Event;
import loghub.jackson.CsvHelpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Csv.Builder.class)
@CanBatch
public class Csv extends AbstractJacksonEncoder<Csv.Builder, CsvMapper> {

    private final Expression[] values;
    private final DatetimeProcessor dateFormat;
    private final ZoneId zoneId;
    private final Charset charset;
    private Csv(Csv.Builder builder) {
        super(builder);
        values = Arrays.copyOf(builder.values, builder.values.length);
        this.zoneId = ZoneId.of(builder.zoneId);
        this.charset = Charset.forName(builder.charset);
        Locale locale = Locale.forLanguageTag(builder.locale);
        this.dateFormat = DatetimeProcessor.of(builder.dateFormat)
                                           .withDefaultZone(zoneId)
                                           .withLocale(locale);
    }

    public static Csv.Builder getBuilder() {
        return new Csv.Builder();
    }

    @Override
    protected JacksonBuilder<CsvMapper> getWriterBuilder(Builder builder) {
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);
        sbuilder.setUseHeader(false);
        sbuilder.setLineSeparator(builder.lineSeparator);
        String[] features = Arrays.stream(builder.features).map(Object::toString).toArray(String[]::new);
        return JacksonBuilder.get(CsvMapper.class)
                             .setSchema(sbuilder.build())
                             .setConfigurator(m -> CsvHelpers.csvGeneratorFeatures(m, features));
    }

    @Override
    public byte[] encode(Event event) throws EncodeException {
        try {
            return writer.writeValueAsString(flattenEvent(event)).getBytes(charset);
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode: " + loghub.Helpers.resolveThrowableException(e), e);
        }
    }

    @Override
    public byte[] encode(Stream<Event> events) throws EncodeException {
        try {
            return writer.writeValueAsString(events.map(this::flattenEvent).collect(Collectors.toList())).getBytes(charset);
        } catch (JsonProcessingException e) {
            throw new EncodeException("Failed to encode: " + loghub.Helpers.resolveThrowableException(e), e);
        }
    }

    private Object[] flattenEvent(Event event) {
        Object[] flattened = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
            try {
                flattened[i] = values[i].eval(event);
                if (flattened[i] instanceof Instant) {
                    flattened[i] = dateFormat.print(ZonedDateTime.from(((Instant) flattened[i]).atZone(zoneId)));
                } else if (flattened[i] instanceof TemporalAccessor) {
                    flattened[i] = dateFormat.print(ZonedDateTime.from((TemporalAccessor) flattened[i]));
                } else if (flattened[i] instanceof Date) {
                    flattened[i] = dateFormat.print(((Date) flattened[i]).toInstant());
                }
            } catch (IgnoredEventException e) {
                flattened[i] = "Missing value";
            } catch (ProcessorException | UnsupportedOperationException e) {
                flattened[i] = e.getCause().getMessage();
            }
        }
        return flattened;
    }

    @Setter
    public static class Builder extends AbstractJacksonEncoder.Builder<Csv> {
        protected String charset = Charset.defaultCharset().name();
        private Expression[] values = new Expression[0];
        private Object[] features = new String[]{"ALWAYS_QUOTE_STRINGS"};
        private char separator = ',';
        private String lineSeparator = "\n";
        private String nullValue = "";
        private String dateFormat = "iso";
        private String zoneId = ZoneId.systemDefault().toString();
        private String locale = Locale.getDefault().toString();

        @Override
        public Csv build() {
            return new Csv(this);
        }
    }

}
