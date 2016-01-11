package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class Drop extends Processor {

    @Override
    public void process(Event event) {
        throw new UnsupportedOperationException("can't process wrapped event");
    }

    @Override
    public String getName() {
        return "Drop";
    }

}
