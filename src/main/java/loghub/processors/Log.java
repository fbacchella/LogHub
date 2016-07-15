package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class Log extends Processor {

    @Override
    public void process(Event event) {
        logger.info("received {}", event);
    }

    @Override
    public String getName() {
        return "log";
    }

}
