package loghub.queue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public abstract class MaybeSemaphore {
    static MaybeSemaphore of(int capacity, boolean blocking) {
        if (blocking) {
            return new LockingSemaphore(capacity);
        } else {
            return new NotBlockingSemaphore();
        }
    }
    abstract boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;
    abstract void acquire() throws InterruptedException;
    abstract void clear();
    abstract void release();
    private static class LockingSemaphore extends MaybeSemaphore {
        private final Semaphore semaphore;
        private final int capacity;
        LockingSemaphore(int capacity) {
            semaphore = new Semaphore(capacity);
            this.capacity = capacity;
        }
        @Override
        boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            return semaphore.tryAcquire(timeout, unit);
        }
        public void acquire() throws InterruptedException {
            semaphore.acquire();
        }

        @Override
        void clear() {
            semaphore.drainPermits();
            semaphore.release(capacity);
        }

        public void release() {
            semaphore.release();
        }
    }
    private static class NotBlockingSemaphore extends MaybeSemaphore {
        NotBlockingSemaphore() {
        }

        @Override
        boolean tryAcquire(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public void acquire() {

        }

        @Override
        void clear() {

        }

        @Override
        public void release() {
        }
    }
}
