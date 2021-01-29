package loghub.senders;

import java.util.concurrent.Semaphore;

import loghub.ConnectionContext;

/**
 * Allows to check that asynchronous acknowledge is indeed being called
 * @author Fabrice Bacchella
 *
 */
public class BlockingConnectionContext extends ConnectionContext<Semaphore> {

    Semaphore lock = new Semaphore(1);

    BlockingConnectionContext() {
        lock.drainPermits();
    }

    @Override
    public Semaphore getLocalAddress() {
        return lock;
    }

    @Override
    public Semaphore getRemoteAddress() {
        return lock;
    }

    @Override
    public void acknowledge() {
        lock.release();
    }

}
