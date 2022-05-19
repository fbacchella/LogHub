package loghub.decoders;

import java.util.Arrays;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.jackson.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Csv.Builder.class)
public class Csv extends AbstractStringJackson<Csv.Builder> {

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
    protected JacksonBuilder<?> getReaderBuilder(Builder builder) {
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        Arrays.stream(builder.columns).forEach(i -> sbuilder.addColumn(i.toString()));
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);
        sbuilder.setUseHeader(builder.header);
        sbuilder.setLineSeparator(builder.lineSeparator);

        return JacksonBuilder.get(CsvMapper.class)
                             .setSchema(sbuilder.build())
                             .setConfigurator(m -> Helpers.csvFeatures(m, builder.features));
    }

}
