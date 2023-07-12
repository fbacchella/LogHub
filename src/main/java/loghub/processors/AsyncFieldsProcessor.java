package loghub.processors;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import io.netty.util.concurrent.Future;
import loghub.AsyncProcessor;
import loghub.ProcessorException;
import loghub.UncheckedProcessorException;
import loghub.VariablePath;
import loghub.configuration.Properties;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

public abstract class AsyncFieldsProcessor<FI, F extends Future<FI>> extends FieldsProcessor {

    private class AsyncFieldSubProcessor extends FieldSubProcessor implements AsyncProcessor<FI, F> {

        AsyncFieldSubProcessor(Iterator<VariablePath> processing) {
            super(processing);
        }

        @Override
        public boolean processCallback(Event event, FI content) throws ProcessorException {
            Supplier<Object> resolver = () -> {
                try {
                    return AsyncFieldsProcessor.this.asyncProcess(event, content);
                } catch (ProcessorException ex) {
                    throw new UncheckedProcessorException(ex);
                }
            };
            try {
                return processField(event, toprocess, resolver, r -> event.putAtPath(resolveDestination(toprocess), r));
            } catch (UncheckedProcessorException ex) {
                throw ex.getProcessorException();
            }
        }

        @Override
        public boolean manageException(Event event, Exception e) throws ProcessorException {
            return AsyncFieldsProcessor.this.manageException(event, e, resolveDestination(toprocess));
        }

        @Override
        public String getName() {
            return String.format("%s$AsyncFieldSubProcessor@%d", AsyncFieldsProcessor.this.getName(), hashCode());
        }

        @Override
        public int getTimeout() {
            return AsyncFieldsProcessor.this.getTimeout();
        }

        @Override
        public BiConsumer<Event, F> getTimeoutHandler() {
            return AsyncFieldsProcessor.this.getTimeoutHandler();
        }

        @Override
        public Optional<Semaphore> getLimiter() {
            return queryCount;
        }

    }

    private class AsyncFieldCollectionProcessor extends DelegatorProcessor implements AsyncProcessor<FI, F> {

        private final List<Object> results;
        private final VariablePath toprocess;
        private final Object value;

        AsyncFieldCollectionProcessor(Object value, List<Object> results, VariablePath toprocess) {
            this.results = results;
            this.toprocess = toprocess;
            this.value = value;
        }

        @Override
        public boolean processCallback(Event event, FI content) throws ProcessorException {
            Supplier<Object> resolver = () -> {
                try {
                    return AsyncFieldsProcessor.this.asyncProcess(event, content);
                } catch (ProcessorException ex) {
                    throw new UncheckedProcessorException(ex);
                }
            };
            try {
                return processField(event, toprocess, resolver, results::add);
            } catch (UncheckedProcessorException ex) {
                throw ex.getProcessorException();
            }
        }

        @Override
        public boolean process(Event event) throws ProcessorException {
            return AsyncFieldsProcessor.this.filterField(event, toprocess, value, results::add);
        }


        @Override
        public boolean manageException(Event event, Exception e) throws ProcessorException {
            return AsyncFieldsProcessor.this.manageException(event, e, resolveDestination(toprocess));
        }

        @Override
        public String getName() {
            return String.format("%s$AsyncFieldCollectionProcessor@%d", AsyncFieldsProcessor.this.getName(), hashCode());
        }

        @Override
        public int getTimeout() {
            return AsyncFieldsProcessor.this.getTimeout();
        }

        @Override
        public BiConsumer<Event, F> getTimeoutHandler() {
            return AsyncFieldsProcessor.this.getTimeoutHandler();
        }

        @Override
        public Optional<Semaphore> getLimiter() {
            return queryCount;
        }

    }

    public abstract static class Builder<AFP extends AsyncFieldsProcessor<FI, F>, FI, F extends Future<FI>> extends FieldsProcessor.Builder<AFP> {
        @Setter
        private int queueDepth = -1;
        @Setter
        private int timeout = 10;
    }

    public abstract Object asyncProcess(Event event, FI content) throws ProcessorException;
    public abstract boolean manageException(Event event, Exception e, VariablePath variablePath) throws ProcessorException;
    public abstract BiConsumer<Event, F> getTimeoutHandler();

    private Optional<Semaphore> queryCount;
    @Getter
    private final int timeout;

    protected AsyncFieldsProcessor(Builder<? extends FieldsProcessor, FI, F> builder) {
            super(builder);
        if (builder.queueDepth == 0 ) {
            queryCount = Optional.empty();
        } else if (builder.queueDepth < 0) {
            // value is negative, not set, will use the default from the properties during the configuration
            queryCount = null;
        } else {
            queryCount = Optional.of(new Semaphore(Math.min(builder.queueDepth, 32768)));
        }
        this.timeout = builder.timeout;
    }

    @Override
    public boolean configure(Properties properties) {
        if (queryCount == null) {
            queryCount = Optional.of(new Semaphore(Math.min(properties.queuesDepth, 32768)));
        }
        return super.configure(properties);
    }

    @Override
    FieldSubProcessor getSubProcessor(Iterator<VariablePath> processing) {
        return new AsyncFieldSubProcessor(processing);
    }

    @Override
    void addCollectionsProcessing(List<Object> values, Event event, VariablePath toprocess, List<Object> results) {
        values.forEach(v -> event.insertProcessor(new AsyncFieldCollectionProcessor(v, results, toprocess)));
    }

    @Override
    boolean doExecution(Event event, VariablePath field) {
        delegate(Collections.singleton(field), event);
        // never reached code
        return false;
    }

}
