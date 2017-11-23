package loghub;

import io.netty.util.concurrent.Future;

public class ProcessorException extends Exception implements ProcessingException {

    public static class DroppedEventException extends ProcessorException {
        public DroppedEventException(Event event) {
            super(event, "dropped");
        }
    };

    public static class PausedEventException extends ProcessorException {
        private final Future<?> future;
        public PausedEventException(Event event, Future<?> future) {
            super(event, "paused");
            this.future = future;
        }
        /**
         * @return the future
         */
        public Future<?> getFuture() {
            return future;
        }
    };

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
