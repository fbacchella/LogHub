package loghub.processors;

import java.util.Arrays;
import java.util.function.Consumer;

import loghub.Event;
import loghub.Processor;
import loghub.configuration.Beans;

@Beans({"field", "fields"})
public abstract class FieldsProcessor extends Processor {
    private String field = "message";
    private String[] fields = new String[]{};

    public abstract void processMessage(Event event, String field);

    @Override
    public void process(Event event) {
        Consumer<String> process = i -> {if( event.containsKey(i) && event.get(i) != null) { processMessage(event, i);};};
        process.accept(field);
        Arrays.stream(fields).forEach(process);
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
