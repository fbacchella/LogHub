package loghub.processors;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import loghub.AsyncProcessor;
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
    private String[] fields = new String[] {};
    private Pattern[] patterns = new Pattern[]{};
    public abstract boolean processMessage(Event event, String field, String destination) throws ProcessorException;

    private class FieldSubProcessor extends Processor {

        final Iterator<String> processing;

        FieldSubProcessor(Iterator<String> processing) {
            this.processing = processing;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            String toprocess = processing.next();
            if (processing.hasNext()) {
                event.insertProcessor(this);
            }
            if (event.get(toprocess) != null) {
                return FieldsProcessor.this.processMessage(event, toprocess, getDestination(toprocess));
            } else {
                throw event.buildException("field " + toprocess + " vanished");
            }
        }

        @Override
        public String getName() {
            return String.format("%s$FieldSubProcessor@%d", FieldsProcessor.this.getName(), hashCode());
        }

        @Override
        public String[] getPathArray() {
            return FieldsProcessor.this.getPathArray();
        }

    }

    private class AsyncFieldSubProcessor extends FieldSubProcessor implements AsyncProcessor<Object> {

        AsyncFieldSubProcessor(Iterator<String> processing) {
            super(processing);
        }

        @Override
        public boolean process(Event event, Object content) throws ProcessorException {
            @SuppressWarnings("unchecked")
            AsyncProcessor<Object> ap = (AsyncProcessor<Object>) FieldsProcessor.this;
            return ap.process(event, content);
        }

        @Override
        public boolean manageException(Event event, Exception e) throws ProcessorException {
            @SuppressWarnings("unchecked")
            AsyncProcessor<Object> ap = (AsyncProcessor<Object>) FieldsProcessor.this;
            return ap.manageException(event, e);
        }

        @Override
        public String getName() {
            return String.format("%s$AsyncFieldSubProcessor@%d", FieldsProcessor.this.getName(), hashCode());
        }

    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        final Set<String> nextfields = new HashSet<>();
        if (patterns.length != 0) {
            //Build a set of fields that needs to be processed
            for (String eventField: new HashSet<>(event.keySet())) {
                for (Pattern p: patterns) {
                    if (p.matcher(eventField).matches() && event.get(eventField) != null) {
                        nextfields.add(eventField);
                        break;
                    }
                }
            }

            // Add a sub processor that will loop on itself until fields are exhausted
            if (nextfields.size() > 0) {
                final Iterator<String> processing = nextfields.iterator();

                Processor fieldProcessor;
                if (this instanceof AsyncProcessor) {
                    fieldProcessor = new AsyncFieldSubProcessor(processing);
                } else {
                    fieldProcessor = new FieldSubProcessor(processing);
                }
                if (processing.hasNext()) {
                    event.insertProcessor(fieldProcessor);
                }
                throw new ProcessorException.IgnoredEventException(event);
            } else {
                return true;
            }
        } else {
            if (event.containsKey(field) && event.get(field) != null) {
                return processMessage(event, field, getDestination(field));
            } else {
                return true;
            }
        }
    }

    private final String getDestination(String srcField) {
        if (destinationFormat == null) {
            return srcField;
        } else {
            return destinationFormat.format(Collections.singletonMap("field", srcField));
        }
    }

    public Object[] getFields() {
        return fields;
    }

    public void setFields(Object[] fields) {
        this.patterns = new Pattern[fields.length];
        for (int i = 0; i < fields.length ; i++) {
            this.patterns[i] = Helpers.convertGlobToRegex(fields[i].toString());
        }
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
