package loghub.processors;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Pattern;

import loghub.AsyncProcessor;
import loghub.Event;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.UncheckedProcessingException;
import loghub.VarFormatter;
import loghub.configuration.Properties;

public abstract class FieldsProcessor extends Processor {

    @Documented
    @Retention(RUNTIME)
    @Target(TYPE)
    @Inherited
    public @interface ProcessNullField {

    }

    protected enum RUNSTATUS {
        FAILED,
        NOSTORE,
        REMOVE
    }

    private String field = "message";
    private VarFormatter destinationFormat = null;
    private String[] fields = new String[] {};
    private Pattern[] patterns = new Pattern[]{};

    public interface AsyncFieldsProcessor<FI> {
        public Object process(Event event, FI content) throws ProcessorException;
        public boolean manageException(Event event, Exception e, String destination) throws ProcessorException;
        public int getTimeout();
    }

    private class FieldSubProcessor extends Processor {

        final Iterator<String> processing;

        // Will be used by AsyncFieldSubProcessor
        protected String toprocess;

        FieldSubProcessor(Iterator<String> processing) {
            this.processing = processing;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            toprocess = processing.next();
            if (processing.hasNext()) {
                event.insertProcessor(this);
            }
            if (event.containsKey(toprocess)) {
                return doProcessMessage(event, toprocess);
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

    private class AsyncFieldSubProcessor<FI> extends FieldSubProcessor implements AsyncProcessor<FI> {

        private final int timeout;

        AsyncFieldSubProcessor(Iterator<String> processing, int timeout) {
            super(processing);
            this.timeout = timeout;
        }

        @Override
        public boolean process(Event event, FI content) throws ProcessorException {
            return FieldsProcessor.this.doProcessMessage(event, toprocess);
        }

        @Override
        public boolean manageException(Event event, Exception e) throws ProcessorException {
            @SuppressWarnings("unchecked")
            AsyncFieldsProcessor<Object> ap = (AsyncFieldsProcessor<Object>) FieldsProcessor.this;
            return ap.manageException(event, e, getDestination(toprocess));
        }

        @Override
        public String getName() {
            return String.format("%s$AsyncFieldSubProcessor@%d", FieldsProcessor.this.getName(), hashCode());
        }

        @Override
        public int getTimeout() {
            return timeout;
        }

    }

    @Override
    public boolean configure(Properties properties) {
        if ( (getFailure() != null || getSuccess() != null || getException() != null) && patterns.length > 0) {
            logger.error("Will not run success or failure");
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (patterns.length != 0) {
            Set<String> nextfields = new HashSet<>();
            //Build a set of fields that needs to be processed
            for (String eventField: new HashSet<>(event.keySet())) {
                for (Pattern p: patterns) {
                    if (p.matcher(eventField).matches()) {
                        nextfields.add(eventField);
                        break;
                    }
                }
            }

            // Add a sub processor that will loop on itself until fields are exhausted
            if (nextfields.size() > 0) {
                delegate(nextfields, event);
                // never reached code
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (this instanceof AsyncFieldsProcessor) {
            // Needed because only AsyncProcessor are allowed to pause
            delegate(Collections.singleton(field), event);
            // never reached code
            return false;
        } else {
            if (event.containsKey(field)) {
                return doProcessMessage(event, field);
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    private boolean doProcessMessage(Event event, String field) throws ProcessorException {
        Object value = event.get(field);
        if (getClass().getAnnotation(ProcessNullField.class) == null && value == null) {
            return false;
        }
        try {
            Object processed = processMessage(event, value);
            if ( ! (processed instanceof RUNSTATUS)) {
                event.put(getDestination(field), processed);
            } else if (processed == RUNSTATUS.REMOVE) {
                event.remove(field);
            }
            return processed != RUNSTATUS.FAILED;
        } catch (ProcessorException.PausedEventException | ProcessorException.DroppedEventException | IgnoredEventException e) {
            throw e;
        } catch (ProcessorException | UncheckedProcessingException e) {
            ProcessorException npe = event.buildException("field " + field + "invalid: " + e.getMessage(), (Exception)e.getCause());
            npe.setStackTrace(e.getStackTrace());
            throw npe;
        }
    }

    public abstract Object processMessage(Event event, Object value) throws ProcessorException;

    private void delegate(Set<String> nextfields, Event event) {
        final Iterator<String> processing = nextfields.iterator();

        Processor fieldProcessor;
        if (this instanceof AsyncFieldsProcessor) {
            fieldProcessor = new AsyncFieldSubProcessor(processing, ((AsyncFieldsProcessor<?>)this).getTimeout());
        } else {
            fieldProcessor = new FieldSubProcessor(processing);
        }
        if (processing.hasNext()) {
            event.insertProcessor(fieldProcessor);
        }
        throw IgnoredEventException.INSTANCE;
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
