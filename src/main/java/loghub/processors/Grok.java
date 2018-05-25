package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;

import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;

public class Grok extends FieldsProcessor {


    public static String PATTERNSFOLDER = "patterns";

    private io.krakens.grok.api.Grok grok;
    private String pattern;
    private Map<Object, Object> customPatterns = Collections.emptyMap();

    @Override
    public boolean configure(Properties properties) {
        GrokCompiler grokCompiler = GrokCompiler.newInstance();

        Helpers.ThrowingConsumer<InputStream> grokloader = is -> grokCompiler.register(new InputStreamReader(new BufferedInputStream(is)));
        try {
            Helpers.readRessources(properties.classloader, PATTERNSFOLDER, grokloader);
            customPatterns.forEach((k,v) -> {
                grokCompiler.register(k.toString(), v.toString());
            });
            grok = grokCompiler.compile(pattern, true);
            // Switch to true when  https://github.com/thekrakken/java-grok/issues/61 is fixed
        } catch (IOException | URISyntaxException | IllegalArgumentException e) {
            logger.error("unable to load patterns: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) {
        if (! event.containsKey(field)) {
            return false;
        }
        String line = event.get(field).toString();
        Match gm = grok.match(line);
        //Results from grok needs to be cleaned
        Map<String, Object> captures = gm.capture();
        if (captures.size() == 0) {
            return false;
        }
        for (Map.Entry<String, Object> e: gm.capture().entrySet()) {
            String destinationField = e.getKey();

            // . is a special field name, it mean a value to put back in the original field
            if (".".equals(e.getKey())) {
                destinationField = field ;
            }
            // Dirty hack to filter non named regex
            // Needed until https://github.com/thekrakken/java-grok/issues/61 is fixed
            if (e.getKey().equals(e.getKey().toUpperCase()) && ! ".".equals(e.getKey())) {
                continue;
            }
            if (e.getValue() == null) {
                continue;
            }
            if (e.getValue() instanceof List) {
                List<?> listvalue = (List<?>) e.getValue();
                List<String> newvalues = new ArrayList<>();
                listvalue.stream().filter( i -> i != null).map(i -> i.toString()).forEach(newvalues::add);
                if (newvalues.size() == 0) {
                    continue;
                } else if (newvalues.size() == 1) {
                    event.put(destinationField, newvalues.get(0));
                } else {
                    event.put(destinationField, newvalues);
                }
            } else {
                event.put(destinationField, e.getValue());
            }
        }
        return true;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public String getName() {
        return "grok";
    }

    /**
     * @return the customPatterns
     */
    public Map<Object, Object> getCustomPatterns() {
        return customPatterns;
    }

    /**
     * @param customPatterns the customPatterns to set
     */
    public void setCustomPatterns(Map<Object, Object> customPatterns) {
        this.customPatterns = customPatterns;
    }

}
