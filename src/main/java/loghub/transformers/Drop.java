package loghub.transformers;

import loghub.Event;
import loghub.Transformer;

public class Drop extends Transformer {

    @Override
    public void transform(Event event) {
        event.dropped = true;
    }

    @Override
    public String getName() {
        return "Drop";
    }

}
