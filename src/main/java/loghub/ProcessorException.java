package loghub;

import io.netty.util.concurrent.Future;
import lombok.Getter;

public class ProcessorException extends Exception implements ProcessingException {

    public static class DroppedEventException extends ProcessorException {
        public DroppedEventException(Event event) {
            super(event, "dropped");
        }
    };

    public static class PausedEventException extends ProcessorException {
        @Getter
        private final Future<?> future;
        public PausedEventException(Event event, Future<?> future) {
            super(event);
            this.future = future;
        }
        public PausedEventException(Event event) {
            super(event);
            this.future = null;
        }
        @Override
        public String getMessage() {
            return "Paused event";
        }
    };

    @Getter
    private final Event event;

    private ProcessorException(Event event) {
        super(null, null, true, false);
        this.event = event;
    }

    ProcessorException(Event event, String message, Exception root) {
        super(message, root);
        this.event = event;
    }

    ProcessorException(Event event, String message) {
        super(message);
        this.event = event;
    }

}
