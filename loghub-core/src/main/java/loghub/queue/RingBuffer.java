package loghub.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

public class RingBuffer<E> {

    private final AtomicLong headCursor = new AtomicLong(0);
    private final AtomicLong tailCursor = new AtomicLong(0);
    private final Semaphore capacitySemaphore;
    private final Semaphore notEmptySemaphore;
    private final AtomicReferenceArray<E> entries;
    private final int capacity;
    private final int capacityMask;

    public RingBuffer(int capacity) {
        // Calculate the next power of 2, greater than or equal to x.
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        this.capacity = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity - 1));
        capacityMask = this.capacity - 1;
        capacitySemaphore = new Semaphore(this.capacity);
        notEmptySemaphore = new Semaphore(0);
        entries = new AtomicReferenceArray<>(this.capacity);
    }

    private boolean compareAndSet(int pos, E refValue, E newValue) {
        return entries.compareAndSet(pos, refValue, newValue);
    }

    private E getAndSet(int pos, E newValue) {
        return entries.getAndSet(pos, newValue);
    }

    private E get(int pos) {
        return entries.get(pos);
    }

    void clear() {
        notEmptySemaphore.drainPermits();
        capacitySemaphore.drainPermits();
        for (int i = 0; i < entries.length(); i++) {
            entries.set(i, null);
        }
        headCursor.set(0);
        tailCursor.set(0);
        capacitySemaphore.release(capacity);
    }

    int remainingCapacity() {
        return entries.length() - (int) (headCursor.get() - tailCursor.get());
    }

    public boolean put(E newEntry, long timeout, TimeUnit timeUnit) {
        try {
            if (capacitySemaphore.tryAcquire(timeout, timeUnit)) {
                int pos = (int) (headCursor.getAndIncrement() & capacityMask);
                while (! compareAndSet(pos, null, newEntry)) {
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

    public boolean put(E newEntry) {
        try {
            capacitySemaphore.acquire();
            int pos = (int) (headCursor.getAndIncrement() & capacityMask);
            while( ! compareAndSet(pos, null, newEntry)) {
                LockSupport.parkNanos(100);
            }
            notEmptySemaphore.release();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public E take() throws InterruptedException {
        notEmptySemaphore.acquire();
        int pos = (int) (tailCursor.getAndIncrement() & capacityMask);
        E entry;
        while ((entry = getAndSet(pos, null)) == null) {
            LockSupport.parkNanos(100);
        }
        capacitySemaphore.release();
        return entry;
    }

    public E poll(long timeout, TimeUnit unit) {
        try {
            if (notEmptySemaphore.tryAcquire(timeout, unit)) {
                int pos = (int) (tailCursor.getAndIncrement() & capacityMask);
                E entry;
                while ((entry =getAndSet(pos, null)) == null) {
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
            while ((entry = getAndSet(pos, null)) == null) {
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
            while ((entry = get(pos)) == null) {
                LockSupport.parkNanos(100);
            }
            notEmptySemaphore.release();
            return entry;
        } else {
            return null;
        }
    }

    public int size() {
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
            int pos = (int) (i & capacityMask);
            E entry;
            while ((entry = get(pos)) == null && i < headCursor.get()) {
                LockSupport.parkNanos(100);
            }
            if (entry == null) {
                throw new NoSuchElementException();
            }
            i++;
            return get(pos);
        }
    }

    public void drainTo(Collection<E> destination) {
        capacitySemaphore.drainPermits();
        notEmptySemaphore.drainPermits();
        for (long i = tailCursor.get() ; i < headCursor.get(); i++) {
            E entry;
            int pos = (int) (i & capacityMask);
            while ((entry = get(pos)) == null && i < headCursor.get()) {
                LockSupport.parkNanos(100);
            }
            destination.add(entry);
        }
        clear();
    }

    public boolean isEmpty() {
        return notEmptySemaphore.availablePermits() == 0;
    }

}
