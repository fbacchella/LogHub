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
import java.util.regex.PatternSyntaxException;

import org.apache.logging.log4j.Level;

import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;

public class Grok extends FieldsProcessor {

    public static String PATTERNSFOLDER = "patterns";

    private final io.thekraken.grok.api.Grok grok;
    private String pattern;
    private Map<Object, Object> customPatterns = Collections.emptyMap();

    public Grok() {
        grok = new io.thekraken.grok.api.Grok();
    }

    @Override
    public boolean configure(Properties properties) {
        Helpers.ThrowingConsumer<InputStream> grokloader = is -> grok.addPatternFromReader(new InputStreamReader(new BufferedInputStream(is)));
        try {
            Helpers.readRessources(properties.classloader, PATTERNSFOLDER, grokloader);
            customPatterns.forEach((k,v) -> {
                try {
                    grok.addPattern(k.toString(), v.toString());
                } catch (GrokException e) {
                    logger.warn("invalid grok pattern {}: {}", k, v);
                }
            });
            // Switch to true when  https://github.com/thekrakken/java-grok/issues/61 is fixed
            grok.compile(pattern, false);
        } catch (IOException | URISyntaxException e) {
            logger.error("unable to load patterns: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return false;
        } catch (GrokException | PatternSyntaxException e) {
            logger.error("wrong pattern {}: {}", pattern, e.getMessage());
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
        gm.captures();
        if (! gm.isNull()) {
            //Results from grok needs to be cleaned
            for (Map.Entry<String, Object> e: gm.toMap().entrySet()) {
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
                    listvalue.stream().filter( i -> i != null).map(i -> i.toString().intern()).forEach(newvalues::add);
                    if (newvalues.size() == 0) {
                        continue;
                    } else if (newvalues.size() == 1) {
                        event.put(destinationField.intern(), newvalues.get(0));
                    } else {
                        event.put(destinationField.intern(), newvalues);
                    }
                } else {
                    event.put(destinationField.intern(), e.getValue());
                }
            }
            return true;
        }
        return false;
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
