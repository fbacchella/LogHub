package loghub.processors;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import loghub.Event;
import loghub.Event.Action;
import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.UncheckedProcessorException;
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

    private String[] field = new String[] {"message"};
    private VarFormatter destinationFormat = null;
    private Pattern[] patterns = new Pattern[]{};
    private String[] globs = new String[] {};

    protected class FieldSubProcessor extends Processor {

        final Iterator<String[]> processing;

        // Will be used by AsyncFieldSubProcessor
        protected String[] toprocess;

        FieldSubProcessor(Iterator<String[]> processing) {
            super(FieldsProcessor.this.logger);
            this.processing = processing;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            toprocess = processing.next();
            if (processing.hasNext()) {
                event.insertProcessor(this);
            }
            boolean containsKey =  Boolean.TRUE.equals(event.applyAtPath(Action.CONTAINS, toprocess, null));
            if (containsKey) {
                return FieldsProcessor.this.filterField(event, toprocess);
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

    @Override
    public boolean configure(Properties properties) {
        if ( (getFailure() != null || getSuccess() != null || getException() != null) && patterns.length > 0) {
            logger.error("Will not run conditionnal processors when multiple fields are defined");
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (patterns.length != 0) {
            Set<String[]> nextfields = new HashSet<>();
            //Build a set of fields that needs to be processed
            for (String eventField: new HashSet<>(event.keySet())) {
                for (Pattern p: patterns) {
                    if (p.matcher(eventField).matches()) {
                        nextfields.add(new String[] {eventField});
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
        } else {
            if (Boolean.TRUE.equals(event.applyAtPath(Action.CONTAINS, field, null))) {
                return doExecution(event, field);
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    boolean doExecution(Event event, String[] currentField) throws ProcessorException {
        return filterField(event, currentField);
    }

    private boolean filterField(Event event, String[] currentField) throws ProcessorException {
        logger.trace("transforming field {} on {}", currentField, event);
        Object value = event.applyAtPath(Action.GET, currentField, null);
        if (getClass().getAnnotation(ProcessNullField.class) == null && value == null) {
            return false;
        }
        Supplier<Object> resolver = () -> {
            try {
                return fieldFunction(event, value);
            } catch (ProcessorException ex) {
                throw new UncheckedProcessorException(ex);
            }
        };
        return processField(event, currentField, resolver);
    }

    protected boolean processField(Event event, String[] currentField, Supplier<Object> resolver) throws ProcessorException {
        try {
            Object processed = resolver.get();
            if ( ! (processed instanceof RUNSTATUS)) {
                event.applyAtPath(Action.PUT, getDestination(currentField), processed);
            } else if (processed == RUNSTATUS.REMOVE) {
                event.applyAtPath(Action.REMOVE, currentField, null);
            }
            return processed != RUNSTATUS.FAILED;
        } catch (UncheckedProcessorException ex) {
            try {
                throw ex.getProcessoException();
            } catch (ProcessorException.DroppedEventException e) {
                throw e;
            } catch (UncheckedProcessorException e) {
                ProcessorException newpe = event.buildException("Field with path \"" + Arrays.toString(currentField) + "\" invalid: " + e.getMessage(), (Exception) e.getProcessoException().getCause());
                newpe.setStackTrace(e.getStackTrace());
                throw newpe;
            } catch (ProcessorException e) {
                ProcessorException newpe = event.buildException("Field with path \"" + Arrays.toString(currentField) + "\" invalid: " + e.getMessage(), (Exception) e.getCause());
                newpe.setStackTrace(e.getStackTrace());
                throw newpe;
            }
        }
    }

    public abstract Object fieldFunction(Event event, Object value) throws ProcessorException;

    void delegate(Set<String[]> nextfields, Event event) {
        Iterator<String[]> processing = nextfields.iterator();
        Processor fieldProcessor = getSubProcessor(processing);
        if (processing.hasNext()) {
            event.insertProcessor(fieldProcessor);
        }
        throw IgnoredEventException.INSTANCE;
    }

    FieldSubProcessor getSubProcessor(Iterator<String[]> processing) {
        return new FieldSubProcessor(processing);
    }

    protected final String[] getDestination(String[] currentField) {
        if (destinationFormat == null) {
            return currentField;
        } else {
            return new String[] {destinationFormat.format(Collections.singletonMap("field", currentField[currentField.length - 1]))};
        }
    }

    public Object[] getFields() {
        return Arrays.copyOf(globs, globs.length);
    }

    public void setFields(Object[] fields) {
        this.globs = new String[fields.length];
        this.patterns = new Pattern[fields.length];
        for (int i = 0 ; i < fields.length ; i++) {
            this.globs[i] = fields[i].toString();
            this.patterns[i] = Helpers.convertGlobToRegex(this.globs[i]);
        }
    }

    public String[] getField() {
        return field;
    }

    public void setField(String[] field) {
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
