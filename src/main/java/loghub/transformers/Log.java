package loghub.transformers;

import java.util.Map;

import loghub.Event;
import loghub.Transformer;

public class Log extends Transformer {

    public Log(Map<String, Event> eventQueue) {
        super(eventQueue);
        setName("TransformerLogger");
    }

    @Override
    public void transform(Event event) {
        System.out.println("received " + event);
    }

}
