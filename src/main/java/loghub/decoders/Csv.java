package loghub.decoders;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.jackson.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(Csv.Builder.class)
public class Csv extends AbstractStringJackson {

    public static class Builder extends AbstractStringJackson.Builder<Csv> {
        @Setter
        private String[] columns = new String[0];
        @Setter
        private String[] features = new String[0];
        @Setter
        private char separator= ',';
        @Setter
        private String nullValue = "";
        @Setter
        private boolean header = false;

        @Override
        public Csv build() {
            return new Csv(this);
        }
    };

    public static Builder getBuilder() {
        return new Builder();
    }

    private final ObjectReader reader;

    protected Csv(Builder builder) {
        super(builder);

        CsvSchema.Builder sbuilder = CsvSchema.builder();
        Arrays.stream(builder.columns).forEach(i -> sbuilder.addColumn(i.toString()));
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);
        sbuilder.setUseHeader(builder.header);

        reader =  JacksonBuilder.get(CsvMapper.class)
                .setMapperSupplier(CsvMapper::new)
                .setSchema(sbuilder.build())
                .setConfigurator(m -> Helpers.csvFeatures(m, builder.features))
                .getReader();
    }

    @Override
    protected Object decodeJackson(ConnectionContext<?> ctx, ObjectResolver gen)
            throws DecodeException, IOException {
        return gen.deserialize(reader);
    }

}
