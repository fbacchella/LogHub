package loghub;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

import io.netty.util.concurrent.Future;
import loghub.events.Event;
import lombok.Getter;

public interface AsyncProcessor<FI, F extends Future<FI>> {
    
    public static class PausedEventException extends RuntimeException {

        @Getter
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

    public boolean processCallback(Event event, FI content) throws ProcessorException;
    public boolean manageException(Event event, Exception e) throws ProcessorException;
    /**
     * Will be called if the event processing timeout. If can be null, but then the processor
     * must handle by his own the timeout, without any external help
     * @return a timeout handler, or null
     */
    public BiConsumer<Event, F> getTimeoutHandler();
    public int getTimeout();
    public default Optional<Semaphore> getLimiter() {
        return Optional.empty();
    }
}
