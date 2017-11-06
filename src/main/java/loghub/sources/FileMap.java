package loghub.sources;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Helpers;
import loghub.Source;
import loghub.configuration.Properties;

public class FileMap extends HashMap<Object, Object> implements Source{

    private static final Logger logger = LogManager.getLogger();
    
    private String name;

    private String mappingFile = null;
    private String key = null;
    private String value = null;
    private String csvFormat = "default";
    private int keyColumn = -1;
    private int valueColumn = -1;

    @Override
    public boolean configure(Properties properties) {
        Map<Object, Object> map = null;
        if (mappingFile == null) {
            logger.error("No mapping source defined");
        }
        switch (Helpers.getMimeType(mappingFile)) {
        case "text/csv":
            map = mapFromCsv();
            break;
        }
        if (map != null) {
            putAll(map);
            return true;
        } else {
            return false;
        }
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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
