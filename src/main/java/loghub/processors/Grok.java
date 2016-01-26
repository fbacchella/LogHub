package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Helpers;
import loghub.configuration.Properties;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;

public class Grok extends FieldsProcessor {

    private static final Logger logger = LogManager.getLogger();

    private final oi.thekraken.grok.api.Grok grok;
    private String pattern;
    private final Map<String, List<String>> reverseIndex = new HashMap<>();

    public Grok() {
        grok = new oi.thekraken.grok.api.Grok();
    }

    @Override
    public void processMessage(Event event, String field, String destination) {
        String line = (String) event.get(field);
        Match gm = grok.match(line);
        gm.captures();
        if(! gm.isNull()) {
            //Many bug in java grok, rebuild the matching manually
            Map<String, String> groups = gm.getMatch().namedGroups();
            Map<String, List<Object>> results = new HashMap<>();
            for(Map.Entry<String, Object> e: gm.toMap().entrySet()) {
                //Dirty hack to filter non named regex
                if(e.getKey().equals(e.getKey().toUpperCase())) {
                    continue;
                }
                List<Object> values = new ArrayList<Object>();
                if( reverseIndex.containsKey(e.getKey())) {
                    for(String subname: reverseIndex.get(e.getKey())) {
                        String subvalue = groups.get(subname);
                        if(subvalue != null) {
                            values.add(subvalue);
                        }
                    }
                } else {
                    results.put(e.getKey(), Collections.singletonList(e.getValue()));
                }
                // Don't keep empty matching
                if(values.size() > 0) {
                    results.put(e.getKey(), values);
                }
            }
            for(Map.Entry<String, List<Object>> e: results.entrySet()) {
                if(e.getValue().size() == 1) {
                    // If only one value found, keep it
                    event.put(e.getKey(), e.getValue().get(0));
                } else {
                    event.put(e.getKey(), e.getValue());
                }
            }
        }
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
            Helpers.readRessources(properties.classloader, "patterns", grokloader);
            grok.compile(pattern);
        } catch (IOException | URISyntaxException e) {
            logger.error("unable to load patterns: {}", e.getMessage());
            logger.catching(Level.DEBUG, e);
            return false;
        } catch (GrokException e) {
            logger.error("wrong pattern {}: {}", pattern, e.getMessage());
            logger.catching(Level.DEBUG, e);
            return false;
        }
        for(Map.Entry<String, String> e: grok.getNamedRegexCollection().entrySet()) {
            if( ! reverseIndex.containsKey(e.getValue())) {
                reverseIndex.put(e.getValue(), new ArrayList<String>());
            }
            reverseIndex.get(e.getValue()).add(e.getKey());
        }
        return super.configure(properties);
    }

}
