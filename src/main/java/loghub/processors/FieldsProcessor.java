package loghub.processors;

import java.util.regex.Pattern;

import loghub.Event;
import loghub.Helpers;
import loghub.Processor;
import loghub.configuration.Beans;

@Beans({"field", "fields"})
public abstract class FieldsProcessor extends Processor {
    private String field = "message";
    private String[] fields = new String[] {};
    private Pattern[] patterns = new Pattern[]{};

    public abstract void processMessage(Event event, String field);

    @Override
    public void process(Event event) {
        if(patterns.length != 0) {
            for(String f: event.keySet()) {
                for(Pattern p: patterns) {
                    if (p.matcher(f).matches() && event.containsKey(f) && event.get(f) != null) {
                        processMessage(event, f);
                        // Processed, don't look for another matchin pattern
                        break;
                    }
                }
            }
        } else {
            if(event.containsKey(field) && event.get(field) != null) {
                processMessage(event, field);
            }
        }
    }

    public Object[] getFields() {
        return fields;
    }

    public void setFields(Object[] fields) {
        this.patterns = new Pattern[fields.length];
        for(int i = 0; i < fields.length ; i++) {
            this.patterns[i] = Helpers.convertGlobToRegex(fields[i].toString());
        }
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
