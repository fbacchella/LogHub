package loghub.transformers;

import loghub.Event;
import loghub.Transformer;

public class Log extends Transformer {

    @Override
    public void transform(Event event) {
        System.out.println("received " + event);
    }

    @Override
    public String getName() {
        return "log";
    }

}
