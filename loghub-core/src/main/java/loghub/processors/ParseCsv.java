package loghub.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.events.Event;
import loghub.jackson.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(ParseCsv.Builder.class)
public class ParseCsv extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<ParseCsv> {
        private String[] columns = new String[0];
        private String[] features = new String[0];
        private char separator = ',';
        @Setter
        private char escapeChar = '\0';
        @Setter
        private String nullValue = "";
        public void setHeaders(String[] headers) {
            columns = Arrays.copyOf(headers, headers.length);
        }
        public void setColumnSeparator(char separator) {
            this.separator = separator;
        }
        public ParseCsv build() {
            return new ParseCsv(this);
        }
    }
    public static ParseCsv.Builder getBuilder() {
        return new ParseCsv.Builder();
    }

    private final ObjectReader reader;

    private ParseCsv(ParseCsv.Builder builder) {
        super(builder);
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        Arrays.stream(builder.columns).forEach(sbuilder::addColumn);
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);

        if (builder.escapeChar != '\0') {
            sbuilder.setEscapeChar(builder.escapeChar);
        }

        reader =  JacksonBuilder.get(CsvMapper.class)
                          .setSchema(sbuilder.build())
                          .setConfigurator(m -> Helpers.csvFeatures(m, builder.features))
                          .getReader();
    }

    @Override
    public Object fieldFunction(Event event, Object value)
                    throws ProcessorException {
        try {
            Map<?, ?> map = reader.readValue(value.toString());
            map.entrySet().stream().forEach(i -> event.put(i.getKey().toString(), i.getValue()));
            return FieldsProcessor.RUNSTATUS.NOSTORE;
        } catch (IOException e) {
            throw event.buildException("failed to parse csv " + value, e);
        }
    }

}
