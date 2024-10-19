package loghub;

import loghub.events.Event;
import lombok.Getter;

@Getter
public class ProcessorException extends Exception implements ProcessingException {

    private final Event event;

    public ProcessorException(Event event, String message, Exception root) {
        super(message, root);
        this.event = event;
    }

    public ProcessorException(Event event, String message) {
        super(message);
        this.event = event;
    }

}
