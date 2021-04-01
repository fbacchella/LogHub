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
import loghub.Event.Action;
import loghub.Helpers;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class Grok extends FieldsProcessor {


    public static final String PATTERNSFOLDER = "patterns";

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
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        Match gm = grok.match(value.toString());
        //Results from grok needs to be cleaned
        Map<String, Object> captures = gm.capture();
        if (captures.size() == 0) {
            return FieldsProcessor.RUNSTATUS.FAILED;
        }
        Object returned = FieldsProcessor.RUNSTATUS.NOSTORE;
        for (Map.Entry<String, Object> e: gm.capture().entrySet()) {
            String destinationField = e.getKey();
            Object stored;
            // Dirty hack to filter non named regex
            // Needed until https://github.com/thekrakken/java-grok/issues/61 is fixed
            if (destinationField.equals(destinationField.toUpperCase()) && ! ".".equals(destinationField)) {
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
                    stored = newvalues.get(0);
                } else {
                    stored = newvalues;
                }
            } else {
                stored = e.getValue();
            }
            // . is a special field name, it mean a value to put back in the original field
            if (! ".".equals(destinationField) ) {
                event.applyAtPath(Action.PUT, Helpers.pathElements(destinationField).stream().toArray(String[]::new), stored, true);
            } else {
                returned = stored;
            }
        }
        return returned;
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
