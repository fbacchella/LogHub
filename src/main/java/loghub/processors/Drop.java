package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class Drop extends Processor {

    @Override
    public void process(Event event) {
        event.dropped = true;
    }

    @Override
    public String getName() {
        return "Drop";
    }

}
