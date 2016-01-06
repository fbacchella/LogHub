package loghub.processors;

import loghub.Event;
import loghub.Processor;
import oi.thekraken.grok.api.Match;
import oi.thekraken.grok.api.exception.GrokException;

public class Grok extends Processor {
    private final oi.thekraken.grok.api.Grok grok;
    private String field;

    public Grok() {
        try {
            grok = oi.thekraken.grok.api.Grok.create("patterns/patterns");
        } catch (GrokException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void process(Event event) {
        String line = (String) event.get(field);

        Match gm = grok.match(line);
        gm.captures();

    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void setPattern(String pattern) throws GrokException {
        grok.compile(pattern);
    }

    public String getPattern() {
        return grok.getOriginalGrokPattern();
    }

    @Override
    public String getName() {
        return "grok";
    }

}
