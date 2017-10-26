package loghub.processors;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import loghub.Event;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class ExternalMap extends FieldsProcessor {

    private String mappingFile = null;
    private String key = null;
    private String value = null;
    private String csvFormat = "default";
    private int keyColumn = -1;
    private int valueColumn = -1;
    private Map<Object, Object> map;

    @Override
    public boolean configure(Properties properties) {
        switch (Helpers.getMimeType(mappingFile)) {
        case "text/csv":
            map = mapFromCsv();
            break;
        }
        return map!= null && super.configure(properties);
    }

    private Map<Object, Object> mapFromCsv() {
        try(Reader in = new FileReader(mappingFile)) {
            CSVFormat format;
            switch (csvFormat.toUpperCase()) {
            case "EXCEL":
                format = CSVFormat.EXCEL;
                break;
            case "RFC4180":
                format = CSVFormat.RFC4180;
                break;
            case "TDF":
                format = CSVFormat.TDF;
                break;
            case "DEFAULT":
                format = CSVFormat.DEFAULT;
                break;
            default:
                logger.error("Unknown CSV format name");
                return null;
            }
            boolean withHeaders;
            if ( key != null && value != null) {
                format = format.withFirstRecordAsHeader();
                withHeaders = true;
            } else if (keyColumn > 0 && valueColumn > 0) {
                format = format.withSkipHeaderRecord(false);
                withHeaders = false;
            } else {
                logger.error("Neither column name or number defined");
                return null;
            }
            Iterable<CSVRecord> records = format.parse(in);
            Map<Object, Object> mapping = new HashMap<>();
            for (CSVRecord record : records) {
                if (withHeaders) {
                    mapping.put(record.get(key), record.get(value));
                } else {
                    mapping.put(record.get(keyColumn), record.get(valueColumn));
                }
            }
            return mapping;
        } catch (IOException e) {
            logger.error("Can't read mapping file {}", mappingFile);
            return null;
        }
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) throws ProcessorException {
        // Yes same code than loghub.processors.Mapper, easier to copy than wrap
        Object key = event.get(field);
        if(key == null) {
            return false;
        }
        // Map only uses integer as key, as parsing number only generate integer
        // So ensure the the key is an integer
        // Ignore float/double case, floating point key don't make sense
        if (key instanceof Number && ! (key instanceof Integer) && ! (key instanceof Double) && ! (key instanceof Double)) {
            key = Integer.valueOf(((Number) key).intValue());
        }
        if (! map.containsKey(key)) {
            return false;
        }
        Object value =  map.get(key);
        event.put(destination, value);
        return true;
    }

    public String getMappingFile() {
        return mappingFile;
    }

    public void setMappingFile(String mappingFile) {
        this.mappingFile = mappingFile;
    }

    public String getKeyName() {
        return key;
    }

    public void setKeyName(String key) {
        this.key = key;
    }

    public String getValueName() {
        return value;
    }

    public void setValueName(String value) {
        this.value = value;
    }

    public String getCsvFormat() {
        return csvFormat;
    }

    public void setCsvFormat(String csvFormat) {
        this.csvFormat = csvFormat;
    }

    public int getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(int keyColumn) {
        this.keyColumn = keyColumn;
    }

    public int getValueColumn() {
        return valueColumn;
    }

    public void setValueColumn(int valueColumn) {
        this.valueColumn = valueColumn;
    }

}
