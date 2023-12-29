package loghub.queue;

import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

class RingBuffer<E> {

    private final AtomicLong headCursor = new AtomicLong(0);
    private final AtomicLong tailCursor = new AtomicLong(0);
    private final Semaphore capacitySemaphore;
    private final Semaphore notEmptySemaphore;
    private final AtomicReference<E>[] entries;
    private final int capacity;
    private final int capacityMask;

    public RingBuffer(int capacity) {
        // Calculate the next power of 2, greater than or equal to x.
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        this.capacity = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity - 1));
        capacityMask = this.capacity - 1;
        capacitySemaphore = new Semaphore(this.capacity);
        notEmptySemaphore = new Semaphore(0);
        entries = new AtomicReference[this.capacity];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = new AtomicReference<>();
        }
    }

    void clear() {
        for (AtomicReference<E> entry : entries) {
            entry.set(null);
        }
        headCursor.set(0);
        tailCursor.set(0);
        notEmptySemaphore.drainPermits();
        capacitySemaphore.drainPermits();
        capacitySemaphore.release(capacity);
    }

    int remainingCapacity() {
        return entries.length - (int) (headCursor.get() - tailCursor.get());
    }

    boolean put(E newEntry, long timeout, TimeUnit timeUnit) {
        try {
            if (capacitySemaphore.tryAcquire(timeout, timeUnit)) {
                int pos = (int) (headCursor.getAndIncrement() & capacityMask);
                while (! entries[pos].compareAndSet(null, newEntry)) {
                    LockSupport.parkNanos(100);
                }
                notEmptySemaphore.release();
                return true;
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    boolean put(E newEntry) {
        try {
            capacitySemaphore.acquire();
            int pos = (int) (headCursor.getAndIncrement() & capacityMask);
            while( ! entries[pos].compareAndSet(null, newEntry)) {
                LockSupport.parkNanos(100);
            }
            notEmptySemaphore.release();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    E take() throws InterruptedException {
        notEmptySemaphore.acquire();
        int pos = (int) (tailCursor.getAndIncrement() & capacityMask);
        E entry;
        while ((entry = entries[pos].getAndSet(null)) == null) {
            LockSupport.parkNanos(100);
        }
        capacitySemaphore.release();
        return entry;
    }

    E poll(long timeout, TimeUnit unit) {
        try {
            if (notEmptySemaphore.tryAcquire(timeout, unit)) {
                int pos = (int) (tailCursor.getAndIncrement() & capacityMask);
                E entry;
                while ((entry = entries[pos].getAndSet(null)) == null) {
                    LockSupport.parkNanos(100);
                }
                capacitySemaphore.release();
                return entry;
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
            int pos = (int) (tailCursor.getAndIncrement() & capacityMask);
            E entry;
            while ((entry = entries[pos].getAndSet(null)) == null) {
                LockSupport.parkNanos(100);
            }
            capacitySemaphore.release();
            return entry;
        } else {
            return null;
        }
    }

    E peek() {
        if (notEmptySemaphore.tryAcquire()) {
            int pos = (int) (tailCursor.get() & capacityMask);
            E entry;
            while ((entry = entries[pos].get()) == null) {
                LockSupport.parkNanos(100);
            }
            notEmptySemaphore.release();
            return entry;
        } else {
            return null;
        }
    }

    int size() {
        return (int) (headCursor.get() - tailCursor.get());
    }

    Iterable<E> iterator() {
        return ElementIterator::new;
    }

    private class ElementIterator implements Iterator<E> {
        private long i = tailCursor.get();
        @Override
        public boolean hasNext() {
            return i < headCursor.get();
        }
        @Override
        public E next() {
            int pos = (int) (i++ % entries.length);
            return entries[pos].get();
        }
    }

    boolean isEmpty() {
        return notEmptySemaphore.availablePermits() == 0;
    }

}
