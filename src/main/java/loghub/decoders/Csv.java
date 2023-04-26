package loghub.decoders;

import java.util.Arrays;

import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.jackson.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Csv.Builder.class)
public class Csv extends AbstractStringJackson<Csv.Builder, CsvMapper> {

    public static class Builder extends AbstractStringJackson.Builder<Csv> {
        @Setter
        private String[] columns = new String[0];
        @Setter
        private String[] features = new String[0];
        @Setter
        private char separator= ',';
        @Setter
        private String lineSeparator= "\n";
        @Setter
        private String nullValue = "";
        @Setter
        private boolean header = false;

        @Override
        public Csv build() {
            return new Csv(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    private Csv(Builder builder) {
        super(builder);
    }

    @Override
    protected JacksonBuilder<CsvMapper> getReaderBuilder(Builder builder) {
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        Arrays.stream(builder.columns).forEach(sbuilder::addColumn);
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);
        sbuilder.setUseHeader(builder.header);
        sbuilder.setLineSeparator(builder.lineSeparator);

        return JacksonBuilder.get(CsvMapper.class)
                             .setSchema(sbuilder.build())
                             .feature(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
                             .setConfigurator(m -> Helpers.csvFeatures(m, builder.features));
    }

}
