package loghub.processors;

import loghub.Processor;
import loghub.events.Event;

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
