package loghub.jackson;

import java.util.Arrays;
import java.util.Locale;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;

public class Helpers {
    
    public static void csvFeatures(CsvMapper mapper, String[] features) {
        Arrays.stream(features).forEach(i -> {
            CsvParser.Feature feature = CsvParser.Feature.valueOf(i.toString().toUpperCase(Locale.ENGLISH));
            mapper.enable(feature);
        });
    }

}
