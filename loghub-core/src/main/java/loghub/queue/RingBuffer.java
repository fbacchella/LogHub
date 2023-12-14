package loghub.queue;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

class RingBuffer<E> implements Iterable<E> {

    private final AtomicLong headCursor = new AtomicLong(0);
    private final AtomicLong tailCursor = new AtomicLong(0);
    private final MaybeSemaphore capacitySemaphore;
    private final Semaphore notEmptySemaphore;
    private final E[] entries;

    public RingBuffer(int capacity, boolean blocking, Class<E> entriesClass) {
        capacitySemaphore = MaybeSemaphore.of(capacity, blocking);
        entries = (E[]) Array.newInstance(entriesClass, capacity);
        notEmptySemaphore = new Semaphore(0);
    }

    void clear() {
        for(int i = 0 ; i < entries.length; i++) {
            entries[i] = null;
        }
        headCursor.set(0);
        tailCursor.set(0);
        notEmptySemaphore.acquireUninterruptibly(notEmptySemaphore.availablePermits());
        capacitySemaphore.clear();
    }

    int remainingCapacity() {
        return entries.length - (int) (headCursor.get() - tailCursor.get());
    }

    boolean put(E entry, long timeout, TimeUnit timeUnit) {
        try {
            capacitySemaphore.tryAcquire(timeout, timeUnit);
            int pos = (int) (headCursor.getAndIncrement() % entries.length);
            entries[pos] = entry;
            notEmptySemaphore.release();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    boolean put(E entry) {
        try {
            capacitySemaphore.acquire();
            int pos = (int) (headCursor.getAndIncrement() % entries.length);
            entries[pos] = entry;
            notEmptySemaphore.release();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    E take() throws InterruptedException {
        notEmptySemaphore.acquire();
        int pos = (int) (tailCursor.getAndIncrement() % entries.length);
        try {
            return entries[pos];
        } finally {
            capacitySemaphore.release();
        }
    }


    E poll(long timeout, TimeUnit unit) {
        try {
            if (notEmptySemaphore.tryAcquire(timeout, unit)) {
                int pos = (int) (tailCursor.getAndIncrement() % entries.length);
                try {
                    return entries[pos];
                } finally {
                    capacitySemaphore.release();
                }
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    E poll() {
        if (notEmptySemaphore.tryAcquire()) {
            int pos = (int) (tailCursor.getAndIncrement() % entries.length);
            try {
                return entries[pos];
            } finally {
                capacitySemaphore.release();
            }
        } else {
            return null;
        }
    }

    E peek() {
        if (notEmptySemaphore.availablePermits() > 0) {
            int pos = (int) (tailCursor.get() % entries.length);
            try {
                return entries[pos];
            } finally {
                capacitySemaphore.release();
            }
        } else {
            return null;
        }
    }

    public int size() {
        return (int) (headCursor.get() - tailCursor.get());
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<>() {
            long i = tailCursor.get();
            @Override
            public boolean hasNext() {
                return i < headCursor.get();
            }

            @Override
            public E next() {
                int pos = (int) (i++ % entries.length);
                return entries[pos];
            }
        };
    }

    public boolean isEmpty() {
        return notEmptySemaphore.availablePermits() == 0;
    }

}
