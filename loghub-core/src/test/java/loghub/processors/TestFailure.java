package loghub.processors;

import org.junit.Test;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestFailure {

    private final EventsFactory factory = new EventsFactory();

    @Test(expected = ProcessorException.class)
    public void test() throws ProcessorException {
        Processor p = new Processor() {

            @Override
            public boolean process(Event event) throws ProcessorException {
                throw event.buildException("test failure", new RuntimeException("test failure"));
            }

            @Override
            public String getName() {
                return null;
            }

        };

        Event event = factory.newEvent();

        event.process(p);

    }

}
