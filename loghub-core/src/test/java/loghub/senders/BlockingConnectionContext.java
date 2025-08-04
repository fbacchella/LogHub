package loghub.senders;

import java.util.concurrent.Semaphore;

import loghub.BuildableConnectionContext;

/**
 * Allows to check that asynchronous acknowledge is indeed being called
 * @author Fabrice Bacchella
 *
 */
public class BlockingConnectionContext extends BuildableConnectionContext<Semaphore> {

    final Semaphore lock = new Semaphore(1);

    public BlockingConnectionContext() {
        lock.drainPermits();
        setOnAcknowledge(lock::release);
    }

    @Override
    public Semaphore getLocalAddress() {
        return lock;
    }

    @Override
    public Semaphore getRemoteAddress() {
        return lock;
    }

}
