package loghub;

public class ProcessorException extends Exception {

    private final Event event;

    ProcessorException(Event event, String message, Exception root) {
        super(message, root);
        this.event = event;
    }

    ProcessorException(Event event, String message) {
        super(message);
        this.event = event;
    }

    /**
     * @return the event
     */
    public Event getEvent() {
        return event;
    }

}
