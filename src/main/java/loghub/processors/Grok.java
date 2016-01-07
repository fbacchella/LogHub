package loghub.processors;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Helpers;
import loghub.Processor;
import loghub.configuration.Properties;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;

public class Grok extends Processor {

    private static final Logger logger = LogManager.getLogger();

    private final oi.thekraken.grok.api.Grok grok;
    private String field;
    private String pattern;

    public Grok() {
        grok = new oi.thekraken.grok.api.Grok();
    }

    @Override
    public void process(Event event) {
        String line = (String) event.get(field);
        Match gm = grok.match(line);
        gm.captures();
        gm.toMap().entrySet().stream()
        .filter( i -> i.getValue() != null)
        .forEach( i-> addElement(event, i.getKey(), i.getValue()));
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
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
        return super.configure(properties);
    }

}
