package loghub;

import loghub.events.Event;

public class UncheckedProcessorException extends RuntimeException implements ProcessingException {

    public UncheckedProcessorException(ProcessorException root) {
        super(root.getMessage(), root);
    }

    /**
     * @return the event
     */
    public Event getEvent() {
        return getProcessorException().getEvent();
    }

    public ProcessorException getProcessorException() {
        return (ProcessorException) getCause();
    }

}
