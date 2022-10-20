package loghub.processors;

import loghub.events.Event;
import loghub.Processor;
import loghub.ProcessorException;

public class Identity extends Processor {

    @Override
    public String getName() {
        return "identity";
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        return true;
    }

}
