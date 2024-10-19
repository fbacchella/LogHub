package loghub;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

import io.netty.util.concurrent.Future;
import loghub.events.Event;
import lombok.Getter;

public interface AsyncProcessor<FI, F extends Future<FI>> {
    
    @Getter
    class PausedEventException extends RuntimeException {

        private final Future<?> future;

        public PausedEventException(Future<?> future) {
            super("Paused event", null, false, false);
            this.future = future;
        }

        public PausedEventException() {
            super("Paused event", null, false, false);
            this.future = null;
        }

    }

    boolean processCallback(Event event, FI content) throws ProcessorException;
    boolean manageException(Event event, Exception e) throws ProcessorException;
    /**
     * Will be called if the event processing timeout. If can be null, but then the processor
     * must handle by his own the timeout, without any external help
     * @return a timeout handler, or null
     */
    BiConsumer<Event, F> getTimeoutHandler();
    int getTimeout();
    default Optional<Semaphore> getLimiter() {
        return Optional.empty();
    }
}
