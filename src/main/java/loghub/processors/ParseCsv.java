package loghub.processors;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import loghub.Event;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class ParseCsv extends FieldsProcessor {

    private String[] columns = new String[0];
    private String[] features = new String[0];
    private char separator= ',';
    private String nullValue = "";

    private CsvSchema schema;
    private final CsvMapper mapper = new CsvMapper();
    private final ThreadLocal<ObjectReader> csv = ThreadLocal.withInitial(() -> mapper.readerFor(HashMap.class).with(schema));

    @Override
    public boolean configure(Properties properties) {
        CsvSchema.Builder builder = CsvSchema.builder();
        Arrays.stream(columns).forEach(i -> builder.addColumn(i.toString()));
        Arrays.stream(features).forEach(i -> {
            CsvParser.Feature feature = CsvParser.Feature.valueOf(i.toString().toUpperCase(Locale.ENGLISH));
            mapper.enable(feature);
        });
        builder.setColumnSeparator(separator);
        builder.setNullValue(nullValue);
        schema = builder.build();
        return super.configure(properties);
    }

    @Override
    public Object fieldFunction(Event event, Object value)
                    throws ProcessorException {
        try {
            Map<?, ?> map = csv.get().readValue(value.toString());
            map.entrySet().stream().forEach( (i) -> event.put(i.getKey().toString(), i.getValue()));
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
     * @return the nullValue
     */
    public String getNullValue() {
        return nullValue;
    }

    /**
     * @param nullValue the nullValue to set
     */
    public void setNullValue(String nullValue) {
        this.nullValue = nullValue;
    }

    /**
     * @return the features
     */
    public String[] getFeatures() {
        return features;
    }

    /**
     * @param features the features to set
     */
    public void setFeatures(String[] features) {
        this.features = features;
    }

}
