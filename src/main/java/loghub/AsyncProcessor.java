package loghub;

import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;

import io.netty.util.concurrent.Future;

public interface AsyncProcessor<FI, F extends Future<FI>> {

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
