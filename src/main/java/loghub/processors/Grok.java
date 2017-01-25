package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
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

    public Grok() {
        grok = new io.thekraken.grok.api.Grok();
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) {
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
                //Dirty hack to filter non named regex
                if (e.getKey().equals(e.getKey().toUpperCase()) && ! ".".equals(e.getKey())) {
                    continue;
                }
                if (e.getValue() == null) {
                    continue;
                }
                if (e.getValue() instanceof List) {
                    List<?> listvalue = (List<?>) e.getValue();
                    List<Object> newvalues = new ArrayList<>();
                    listvalue.stream().filter( i -> i != null).forEach(newvalues::add);
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

    @Override
    public boolean configure(Properties properties) {
        Helpers.ThrowingConsumer<InputStream> grokloader = is -> grok.addPatternFromReader(new InputStreamReader(new BufferedInputStream(is)));
        try {
            Helpers.readRessources(properties.classloader, PATTERNSFOLDER, grokloader);
            grok.compile(pattern);
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

}
