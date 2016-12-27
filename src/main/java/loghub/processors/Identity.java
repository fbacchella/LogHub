package loghub.processors;

import loghub.Event;

public class Identity extends FieldsProcessor {


    @Override
    public String getName() {
        return "identity";
    }

    @Override
    public boolean processMessage(Event event, String field, String destination) {
        return true;
    }

}
