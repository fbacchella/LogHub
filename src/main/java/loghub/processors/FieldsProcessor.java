package loghub.processors;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import loghub.Helpers;
import loghub.IgnoredEventException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.UncheckedProcessorException;
import loghub.VarFormatter;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public abstract class FieldsProcessor extends Processor {

    @Documented
    @Retention(RUNTIME)
    @Target(TYPE)
    @Inherited
    public @interface ProcessNullField {

    }

    @Documented
    @Retention(RUNTIME)
    @Target(TYPE)
    @Inherited
    public @interface InPlace {

    }

    protected enum RUNSTATUS {
        FAILED,
        NOSTORE,
        REMOVE
    }

    private static final VariablePath DEFAULT_FIELD = VariablePath.of("message");

    public abstract static class Builder<FP extends FieldsProcessor> extends Processor.Builder<FP> {
        private VariablePath destination;
        private VarFormatter destinationTemplate;
        @Setter
        private VariablePath field = DEFAULT_FIELD;
        @Setter
        private String[] fields;
        @Setter
        private boolean inPlace = false;
        @Setter
        private boolean iterate = true;
        public void setDestination(VariablePath destination) {
            this.destination = destination;
            if (destination != null) {
                this.destinationTemplate = null;
                this.inPlace = false;
            }
        }
        public void setDestinationTemplate(VarFormatter destinationTemplate) {
            this.destinationTemplate = destinationTemplate;
            if (destinationTemplate != null) {
                this.destination = null;
                this.inPlace = false;
            }
        }
    }

    private VariablePath field = DEFAULT_FIELD;
    private Pattern[] patterns = new Pattern[]{};
    private String[] globs = new String[] {};
    @Getter
    private VariablePath destination = null;
    @Getter
    private VarFormatter destinationTemplate = null;
    @Getter
    private boolean inPlace = false;
    @Getter @Setter
    private boolean iterate = false;

    protected abstract class DelegatorProcessor extends Processor {

        public DelegatorProcessor() {
            super(FieldsProcessor.this.logger);
        }

        @Override
        public Processor getFailure() {
            return FieldsProcessor.this.getFailure();
        }

        @Override
        public Processor getSuccess() {
            return FieldsProcessor.this.getSuccess();
        }

        @Override
        public Processor getException() {
            return FieldsProcessor.this.getException();
        }

    }

    protected class FieldSubProcessor extends DelegatorProcessor {

        final Iterator<VariablePath> processing;

        // Will be used by AsyncFieldSubProcessor
        protected VariablePath toprocess;

        FieldSubProcessor(Iterator<VariablePath> processing) {
            super();
            this.processing = processing;
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            toprocess = processing.next();
            if (processing.hasNext()) {
                // More variables to process, so reinsert this process
                event.insertProcessor(this);
            }
            if (! event.containsAtPath(toprocess)) {
                throw event.buildException("Field " + toprocess + " vanished");
            } else if (isIterable(event, toprocess)){
                Object value = event.getAtPath(toprocess);
                List<Object> values;
                if (value instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    Collection<Object> c = (Collection<Object>)value;
                    values = new ArrayList<>(c);
                 } else {
                    values = Arrays.stream((Object[])value).collect(Collectors.toList());
                }
                List<Object> results = new ArrayList<>((values).size() * 2);
                Collections.reverse(values);
                event.insertProcessor(new Processor() {
                    @Override
                    public boolean process(Event event) {
                        event.putAtPath(resolveDestination(toprocess), results);
                        return false;
                    }
                });
                addCollectionsProcessing(values, event, toprocess, results);
                throw IgnoredEventException.INSTANCE;
            } else {
                return FieldsProcessor.this.filterField(event, toprocess, event.getAtPath(toprocess), r -> event.putAtPath(resolveDestination(toprocess), r));
            }
        }

        @Override
        public String getName() {
            return String.format("%s$FieldSubProcessor@%d", FieldsProcessor.this.getName(), hashCode());
        }

        @Override
        public VariablePath getPathArray() {
            return FieldsProcessor.this.getPathArray();
        }

    }

    protected FieldsProcessor(Builder<? extends FieldsProcessor> builder) {
        super(builder);
        this.inPlace = builder.inPlace && getClass().getAnnotation(InPlace.class) != null;
        this.iterate = builder.iterate;
        if (inPlace) {
            this.destinationTemplate = null;
            this.destination = null;
        } else {
            this.destinationTemplate = builder.destinationTemplate;
            this.destination = builder.destination;
        }
        this.field = Optional.ofNullable(builder.field)
                             .orElse(VariablePath.EMPTY);
        if (builder.fields != null) {
            this.globs = new String[builder.fields.length];
            this.patterns = new Pattern[builder.fields.length];
            for (int i = 0 ; i < builder.fields.length ; i++) {
                this.globs[i] = builder.fields[i];
                this.patterns[i] = Helpers.convertGlobToRegex(this.globs[i]);
            }
        } else {
            this.globs = new String[0];
            this.patterns = new Pattern[0];
        }
    }

    protected FieldsProcessor() {
        // Empty constructor
    }

    @Override
    public boolean configure(Properties properties) {
        if ( (getFailure() != null || getSuccess() != null || getException() != null) && patterns.length > 0) {
            logger.error("Will not run conditional processors when multiple fields are defined");
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (patterns.length != 0) {
            // Patterns found, try to process many variables
            Set<VariablePath> nextfields = new HashSet<>();
            //Build a set of fields that needs to be processed
            for (String eventField: new HashSet<>(event.keySet())) {
                for (Pattern p: patterns) {
                    if (p.matcher(eventField).matches()) {
                        nextfields.add(VariablePath.of(eventField));
                        break;
                    }
                }
            }

            // Add a sub processor that will loop on itself until fields are exhausted
            if (!nextfields.isEmpty()) {
                delegate(nextfields, event);
            }
            throw IgnoredEventException.INSTANCE;
        } else if (! event.containsAtPath(field)) {
            throw IgnoredEventException.INSTANCE;
        } else if (isIterable(event, field)) {
            // The returned value is iterable and iteration for each value was requested, delegate it
            delegate(Set.of(field), event);
            throw IgnoredEventException.INSTANCE;
        } else {
            // A single variable to process
            return doExecution(event, field);
        }
    }

    boolean doExecution(Event event, VariablePath currentField) throws ProcessorException {
        return filterField(event, currentField, event.getAtPath(currentField), v -> event.putAtPath(resolveDestination(currentField), v));
    }

    boolean filterField(Event event, VariablePath currentField, Object value, Consumer<Object> postProcess) throws ProcessorException {
        logger.trace("Processing field {} on {}", currentField, event);
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
        return processField(event, currentField, resolver, postProcess);
    }

    boolean processField(Event event, VariablePath currentField, Supplier<Object> resolver,
            Consumer<Object> postProcess) throws ProcessorException {
        try {
            Object processed = resolver.get();
            if (processed instanceof Map && inPlace) {
                event.putAll((Map) processed);
            } else if (! (processed instanceof RUNSTATUS)) {
                postProcess.accept(processed);
            } else if (processed == RUNSTATUS.REMOVE) {
                event.removeAtPath(currentField);
            }
            return processed != RUNSTATUS.FAILED;
        } catch (UncheckedProcessorException ex) {
            try {
                throw ex.getProcessorException();
            } catch (ProcessorException.DroppedEventException e) {
                throw e;
            } catch (UncheckedProcessorException e) {
                ProcessorException newpe = event.buildException("Field with path \"" + currentField.toString() + "\" invalid: " + e.getMessage(), (Exception) e.getProcessorException().getCause());
                newpe.setStackTrace(e.getStackTrace());
                throw newpe;
            } catch (ProcessorException e) {
                ProcessorException newpe = event.buildException("Field with path \"" + currentField.toString() + "\" invalid: " + e.getMessage(), (Exception) e.getCause());
                newpe.setStackTrace(e.getStackTrace());
                throw newpe;
            }
        }
    }

    protected VariablePath resolveDestination(VariablePath currentField) {
        if (destinationTemplate != null) {
            return VariablePath.of(
                    destinationTemplate.format(Collections.singletonMap("field", currentField.get(currentField.length() -1))));
        } else if (destination != null) {
            return destination;
        } else {
            return currentField;
        }
    }

    public abstract Object fieldFunction(Event event, Object value) throws ProcessorException;

    void delegate(Set<VariablePath> nextfields, Event event) {
        Iterator<VariablePath> processing = nextfields.iterator();
        Processor fieldProcessor = getSubProcessor(processing);
        if (processing.hasNext()) {
            event.insertProcessor(fieldProcessor);
        }
    }

    protected boolean isIterable(Event event, VariablePath vp) {
        if (! event.containsAtPath(vp)) {
            return false;
        } else {
            Object value = event.getAtPath(vp);
            return iterate && value != null && (value instanceof Collection || value.getClass().isArray());
        }
    }

    FieldSubProcessor getSubProcessor(Iterator<VariablePath> processing) {
        return new FieldSubProcessor(processing);
    }

    void addCollectionsProcessing(List<Object> values, Event event, VariablePath toprocess, List<Object> results) {
        values.forEach(v -> event.insertProcessor(new Processor() {
            @Override
            public boolean process(Event ev) throws ProcessorException {
                return FieldsProcessor.this.filterField(event, toprocess, v, results::add);
            }
        }));
    }

    public String[] getFields() {
        return Arrays.copyOf(globs, globs.length);
    }

    public void setFields(String[] fields) {
        this.globs = new String[fields.length];
        this.patterns = new Pattern[fields.length];
        for (int i = 0 ; i < fields.length ; i++) {
            this.globs[i] = fields[i].toString();
            this.patterns[i] = Helpers.convertGlobToRegex(this.globs[i]);
        }
    }

    public VariablePath getField() {
        return field;
    }

    public void setField(VariablePath field) {
        this.field = field;
    }

    public void setInPlace(boolean inPlace) {
        this.inPlace = inPlace;
        if (inPlace) {
            destination = null;
            destinationTemplate = null;
        }
    }

    public void setDestination(VariablePath destination) {
        this.destination = destination;
        if (destination != null) {
            this.destinationTemplate = null;
            this.inPlace = false;
        }
    }

    public void setDestinationTemplate(VarFormatter destinationTemplate) {
        this.destinationTemplate = destinationTemplate;
        if (destinationTemplate != null) {
            this.destination = null;
            this.inPlace = false;
        }
    }

}
