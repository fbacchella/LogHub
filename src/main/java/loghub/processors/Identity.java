package loghub.processors;

import loghub.Event;

public class Identity extends FieldsProcessor {


    @Override
    public String getName() {
        return "identity";
    }

    @Override
    public void processMessage(Event event, String field) {
    }

}
