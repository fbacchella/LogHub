package loghub.sources;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.json.JsonMapper;

import loghub.AbstractBuilder;
import loghub.BuilderClass;
import loghub.Helpers;
import loghub.configuration.Properties;
import loghub.jackson.JacksonBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;

@BuilderClass(FileMap.Builder.class)
public class FileMap extends HashMap<Object, Object> implements Source {

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class Builder extends AbstractBuilder<FileMap> {
        @Setter
        private String mappingFile = null;
        @Setter
        private String csvFormat = "default";
        @Setter
        private String keyName = null;
        @Setter
        private String valueName = null;
        @Setter
        private int keyColumn = -1;
        @Setter
        private int valueColumn = -1;
        @Override
        public FileMap build() {
            return new FileMap(this);
        }
    }

    public static FileMap.Builder getBuilder() {
        return new FileMap.Builder();
    }

    private FileMap(Builder builder) {
        if (builder.mappingFile == null) {
            throw new IllegalArgumentException("No mapping source defined");
        } else {
            String mimeType = Helpers.getMimeType(builder.mappingFile);
            switch (mimeType) {
            case "text/csv":
                mapFromCsv(builder);
                break;
            case "application/json":
                mapFromJson(builder);
                break;
            default:
                throw new IllegalArgumentException("Unhandled MIME type " + mimeType);
            }
        }
    }

    @Override
    public boolean configure(Properties properties) {
        return ! isEmpty();
    }

    private void mapFromJson(Builder builder) {
        try (Reader in = new FileReader(builder.mappingFile)) {
            ObjectReader reader = JacksonBuilder.get(JsonMapper.class).getReader();
            putAll(reader.readValue(in, Map.class));
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't read mapping file " + builder.mappingFile);
        }
    }

    private void mapFromCsv(Builder builder) {
        try (Reader in = new FileReader(builder.mappingFile)) {
            CSVFormat.Builder fbuilder;
            switch (builder.csvFormat.toUpperCase()) {
            case "EXCEL":
                fbuilder = CSVFormat.EXCEL.builder();
                break;
            case "RFC4180":
                fbuilder = CSVFormat.RFC4180.builder();
                break;
            case "TDF":
                fbuilder = CSVFormat.TDF.builder();
                break;
            case "DEFAULT":
                fbuilder = CSVFormat.DEFAULT.builder();
                break;
            default:
                throw new IllegalArgumentException("Unknown CSV format name: " + builder.csvFormat);
            }
            boolean withHeaders;
            if (builder.keyName != null && builder.valueName != null) {
                fbuilder.setHeader().setSkipHeaderRecord(true);
                withHeaders = true;
            } else if (builder.keyColumn > 0 && builder.valueColumn > 0) {
                fbuilder.setSkipHeaderRecord(false);
                withHeaders = false;
            } else {
                throw new IllegalArgumentException("Neither column name or number defined");
            }
            Iterable<CSVRecord> records = fbuilder.build().parse(in);
            for (CSVRecord csvRecord : records) {
                if (withHeaders) {
                    put(csvRecord.get(builder.keyName), csvRecord.get(builder.valueName));
                } else {
                    put(csvRecord.get(builder.keyColumn), csvRecord.get(builder.valueColumn));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Can't read mapping file " + builder.mappingFile + ": " + Helpers.resolveThrowableException(e), e);
        }
    }

}
