package loghub.processors;

import loghub.Processor;
import loghub.events.Event;

public class Identity extends Processor {

    @Override
    public String getName() {
        return "identity";
    }

    @Override
    public boolean process(Event event) {
        return true;
    }

}
