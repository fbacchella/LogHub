package loghub.processors;

import loghub.Event;
import loghub.Processor;

public class Log extends Processor {

    @Override
    public void process(Event event) {
        System.out.println("received " + event);
    }

    @Override
    public String getName() {
        return "log";
    }

}
