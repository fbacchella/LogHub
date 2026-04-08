package loghub.processors;

import java.io.IOException;
import java.io.StringReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import loghub.Processor;
import loghub.ProcessorException;
import loghub.Tools;
import loghub.configuration.Configuration;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.events.EventsFactory;
import lombok.Setter;

class TestFailure {

    private final EventsFactory factory = new EventsFactory();

    public static class FailedProcessor extends Processor {
        @Setter
        private int exceptionType;
        @Override
        public boolean process(Event event) throws ProcessorException {
            switch (exceptionType) {
            case 1 -> {
                return true;
            }
            case 2 -> {
                return false;
            }
            default -> throw event.buildException("test failure", new RuntimeException());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void test(int exceptionType) throws IOException {
        String conf = String.format("""
        pipeline[main] {
            loghub.processors.TestFailure$FailedProcessor {
                exceptionType: %d,
                success: [a] = 1,
                failure: [a] = 2,
                exception: [a] = 3,
            }
        }
        """, exceptionType);
        Properties p = Configuration.parse(new StringReader(conf));
        Processor m = p.namedPipeLine.get("main").processors.stream().findFirst().get();
        m.configure(p);
        Event event = factory.newEvent();
        Tools.runProcessing(event, p.namedPipeLine.get("main"), p);
        Assertions.assertEquals(exceptionType, event.get("a"));
    }

}
