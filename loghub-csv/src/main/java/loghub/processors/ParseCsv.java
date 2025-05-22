package loghub.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.BuilderClass;
import loghub.ProcessorException;
import loghub.VariablePath;
import loghub.events.Event;
import loghub.jackson.CsvHelpers;
import loghub.jackson.JacksonBuilder;
import lombok.Setter;

@BuilderClass(ParseCsv.Builder.class)
public class ParseCsv extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<ParseCsv> {
        private VariablePath[] columns = new VariablePath[0];
        private String[] features = new String[0];
        private char separator = ',';
        @Setter
        private char escapeChar = '\0';
        @Setter
        private String nullValue = "";
        public void setHeaders(VariablePath[] headers) {
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
    private final VariablePath[] columns;

    private ParseCsv(ParseCsv.Builder builder) {
        super(builder);
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        sbuilder.setColumnSeparator(builder.separator);
        sbuilder.setNullValue(builder.nullValue);
        sbuilder.setUseHeader(false);
        CsvSchema schema = sbuilder.build();
        columns = builder.columns;
        if (builder.escapeChar != '\0') {
            sbuilder.setEscapeChar(builder.escapeChar);
        }

        reader =  JacksonBuilder.get(CsvMapper.class)
                                .setSchema(schema)
                                .setConfigurator(m -> CsvHelpers.csvFeatures(m, builder.features))
                                .setTypeReference(new TypeReference<List<String>>() {})
                                .getReader();
    }

    @Override
    public Object fieldFunction(Event event, Object value)
                    throws ProcessorException {
        try {
           List<String> values = reader.readValue(value.toString());
            for (int i = 0 ; i < Math.min(columns.length, values.size()); i++) {
                String columnValue = values.get(i);
                if (columnValue == null) {
                    continue;
                }
                if (columnValue.charAt(0) == '"' && columnValue.charAt(columnValue.length() - 1) == '"') {
                    columnValue = columnValue.substring(1, columnValue.length() - 1);
                }
                event.putAtPath(columns[i], columnValue);
            }
            return FieldsProcessor.RUNSTATUS.NOSTORE;
        } catch (IOException e) {
            throw event.buildException("failed to parse csv " + value, e);
        }
    }

}
