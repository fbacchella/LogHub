package loghub.processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.regex.Pattern;

import loghub.Event;
import loghub.Helpers;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.VarFormatter;
import loghub.configuration.Beans;

@Beans({"field", "fields"})
public abstract class FieldsProcessor extends Processor {
    private String field = "message";
    private VarFormatter destinationFormat = null;
    private String fields = null;
    private Pattern patterns = null;

    public abstract boolean processMessage(Event event, String field, String destination) throws ProcessorException;

    @Override
    public boolean process(Event event) throws ProcessorException {
        boolean success = false;
        if (patterns != null) {
            for (String f: new HashSet<>(event.keySet())) {
                if (patterns.matcher(f).matches() && event.containsKey(f) && event.get(f) != null) {
                    success |= processMessage(event, f, getDestination(f));
                }
            }
        } else {
            if (event.containsKey(field) && event.get(field) != null) {
                success |= processMessage(event, field, getDestination(field));
            }
        }
        return success;
    }

    private final String getDestination(String field) {
        if (destinationFormat == null) {
            return field;
        } else {
            return destinationFormat.format(Collections.singletonMap("field", field));
        }
    }

    public String getFields() {
        return fields;
    }

    public void setFields(String fields) {
        this.patterns = Helpers.convertGlobToRegex(fields);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return the destination
     */
    public String getDestination() {
        return destinationFormat.toString();
    }

    /**
     * @param destination the destination to set
     */
    public void setDestination(String destination) {
        this.destinationFormat = new VarFormatter(destination);
    }

}
