package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class Identity extends Processor {

    @Override
    public void process(Event event) {
    }

    @Override
    public String getName() {
        return "identity";
    }

}
