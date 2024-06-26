package loghub.processors;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.events.Event;

public class UnstackException extends Processor {

    @Override
    public boolean process(Event event) throws ProcessorException {
        event.popException();
        return true;
    }

}
