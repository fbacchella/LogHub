package loghub.transformers;

import loghub.Event;
import loghub.Transformer;

public class Identity extends Transformer {

    @Override
    public void transform(Event event) {
    }

    @Override
    public String getName() {
        return "identity";
    }

}
