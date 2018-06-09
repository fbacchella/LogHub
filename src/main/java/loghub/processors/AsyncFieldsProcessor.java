package loghub.processors;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Supplier;

import loghub.AsyncProcessor;
import loghub.Event;
import loghub.Processor;
import loghub.ProcessorException;

public abstract class AsyncFieldsProcessor<FI> extends FieldsProcessor {

    private class AsyncFieldSubProcessor extends FieldSubProcessor implements AsyncProcessor<FI> {

        AsyncFieldSubProcessor(Iterator<String> processing) {
            super(processing);
        }

        @Override
        public boolean processCallback(Event event, FI content) throws ProcessorException {
            Supplier<Object> resolver = () -> {
                try {
                    return AsyncFieldsProcessor.this.asyncProcess(event, content);
                } catch (ProcessorException ex) {
                    throw new RuntimeException(ex);
                }
            };
            return processField(event, toprocess, resolver);
        }

        @Override
        public boolean manageException(Event event, Exception e) throws ProcessorException {
            return AsyncFieldsProcessor.this.manageException(event, e, getDestination(toprocess));
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
        public Processor getFailure() {
            return AsyncFieldsProcessor.this.getFailure();
        }

        @Override
        public Processor getSuccess() {
            return AsyncFieldsProcessor.this.getSuccess();
        }

        @Override
        public Processor getException() {
            return AsyncFieldsProcessor.this.getException();
        }

    }

    private int timeout = 10;

    public abstract Object asyncProcess(Event event, FI content) throws ProcessorException;
    public abstract boolean manageException(Event event, Exception e, String destination) throws ProcessorException;

    @Override
    FieldSubProcessor getSubProcessor(Iterator<String> processing) {
        return new AsyncFieldSubProcessor(processing);
    }

    boolean doExecution(Event event, String field) throws ProcessorException {
        delegate(Collections.singleton(field), event);
        // never reached code
        return false;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

}
