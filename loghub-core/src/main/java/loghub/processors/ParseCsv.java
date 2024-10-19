package loghub.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.ProcessorException;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.jackson.Helpers;
import loghub.jackson.JacksonBuilder;
import lombok.Getter;
import lombok.Setter;

public class ParseCsv extends FieldsProcessor {

    private String[] columns = new String[0];
    @Getter
    private String[] features = new String[0];
    private char separator= ',';
    @Getter @Setter
    private char escapeChar = '\0';
    @Getter
    private String nullValue = "";
    private ObjectReader reader;

    @Override
    public boolean configure(Properties properties) {
        CsvSchema.Builder sbuilder = CsvSchema.builder();
        Arrays.stream(columns).forEach(sbuilder::addColumn);
        sbuilder.setColumnSeparator(separator);
        sbuilder.setNullValue(nullValue);

        if (escapeChar != '\0') {
            sbuilder.setEscapeChar(escapeChar);
        }

        reader =  JacksonBuilder.get(CsvMapper.class)
                                .setSchema(sbuilder.build())
                                .setConfigurator(m -> Helpers.csvFeatures(m, features))
                                .getReader();

        return super.configure(properties);
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

    public String[] getHeaders() {
        return columns;
    }

    public void setHeaders(String[] columns) {
        this.columns = columns;
    }

    public Character getColumnSeparator() {
        return separator;
    }

    public void setColumnSeparator(Character separator) {
        this.separator = separator;
    }

    /**
     * @param nullValue the nullValue to set
     */
    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    /**
     * @param features the features to set
     */
    public void setFeatures(String[] features) {
        this.features = features;
    }

}
