package loghub.processors;

import org.junit.Test;

import loghub.Event;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;

public class TestFailure {

    @Test(expected=ProcessorException.class)
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

        Event event = Tools.getEvent();

        event.process(p);

    }

}
