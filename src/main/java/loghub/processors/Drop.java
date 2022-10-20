package loghub.processors;

import loghub.events.Event;
import loghub.Processor;

public class Drop extends Processor {

    @Override
    public boolean process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    @Override
    public String getName() {
        return "Drop";
    }

}
