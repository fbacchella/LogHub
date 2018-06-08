package loghub.processors;

import java.util.Iterator;

import loghub.AsyncProcessor;
import loghub.Event;
import loghub.ProcessorException;

public abstract class AsyncFieldsProcessor<FI> extends FieldsProcessor {

    private class AsyncFieldSubProcessor extends FieldSubProcessor implements AsyncProcessor<FI> {

        AsyncFieldSubProcessor(Iterator<String> processing) {
            super(processing);
        }

        @Override
        public boolean processCallback(Event event, FI content) throws ProcessorException {
            return AsyncFieldsProcessor.this.process(event, content, AsyncFieldsProcessor.this.getDestination(toprocess));
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

    }

    public abstract boolean process(Event event, FI content, String destination) throws ProcessorException;
    public abstract boolean manageException(Event event, Exception e, String destination) throws ProcessorException;
    public abstract int getTimeout();

    @Override
    protected FieldSubProcessor getSubProcessor(Iterator<String> processing) {
        return new AsyncFieldSubProcessor(processing);
    }

}
