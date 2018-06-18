package loghub;

public class UncheckedProcessorException extends RuntimeException implements ProcessingException {

    public UncheckedProcessorException(ProcessorException root) {
        super(root.getMessage(), root);
    }

    /**
     * @return the event
     */
    public Event getEvent() {
        return getProcessoException().getEvent();
    }

    public ProcessorException getProcessoException() {
        return (ProcessorException) getCause();
    }

}
