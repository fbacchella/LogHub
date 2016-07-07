package loghub.processors;

import org.junit.Test;

import loghub.Event;
import loghub.EventWrapper;
import loghub.Processor;
import loghub.ProcessorException;

public class TestFailure {

    @Test(expected=ProcessorException.class)
    public void test() throws ProcessorException {
        Processor p = new Processor() {

            @Override
            public void process(Event event) throws ProcessorException {
                throw new ProcessorException("test failure", new RuntimeException("test failure"));
            }

            @Override
            public String getName() {
                return null;
            }

        };

        EventWrapper event = new EventWrapper(new Event());

        event.setProcessor(p);

        p.process(event);

    }

}
