package loghub.queue;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

class RingBuffer<E> {

    private final AtomicLong headCursor = new AtomicLong(0);
    private final AtomicLong tailCursor = new AtomicLong(0);
    private final Semaphore capacitySemaphore;
    private final Semaphore notEmptySemaphore;
    private final E[] entries;
    private final Consumer<E> cleaner;

    public RingBuffer(int capacity, Class<E> entriesClass, Supplier<E> entriesSupplier, Consumer<E> cleaner) {
        // Calculate the next power of 2, greater than or equal to x.
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        capacity = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity - 1));
        capacitySemaphore = new Semaphore(capacity);
        notEmptySemaphore = new Semaphore(0);
        entries = arrayInstance(entriesClass, capacity);
        for (int i = 0; i < capacity; i++) {
            entries[i] = entriesSupplier.get();
        }
        this.cleaner = cleaner;
    }

    @SuppressWarnings("unchecked")
    private E[] arrayInstance(Class<E> entriesClass, int capacity) {
        return (E[]) Array.newInstance(entriesClass, capacity);
    }

    void clear() {
        for (E entry : entries) {
            cleaner.accept(entry);
        }
        headCursor.set(0);
        tailCursor.set(0);
        notEmptySemaphore.acquireUninterruptibly(notEmptySemaphore.availablePermits());
        capacitySemaphore.drainPermits();
        capacitySemaphore.release(entries.length);
    }

    int remainingCapacity() {
        return entries.length - (int) (headCursor.get() - tailCursor.get());
    }

    boolean put(Consumer<E> setter, long timeout, TimeUnit timeUnit) {
        try {
            if (capacitySemaphore.tryAcquire(timeout, timeUnit)) {
                int pos = (int) (headCursor.getAndIncrement() % entries.length);
                synchronized (entries[pos]) {
                    setter.accept(entries[pos]);
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

    boolean put(Consumer<E> setter) {
        try {
            capacitySemaphore.acquire();
            int pos = (int) (headCursor.getAndIncrement() % entries.length);
            synchronized (entries[pos]) {
                setter.accept(entries[pos]);
            }
            notEmptySemaphore.release();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    <V> V take(Function<E, V> extract) throws InterruptedException {
        notEmptySemaphore.acquire();
        int pos = (int) (tailCursor.getAndIncrement() % entries.length);
        synchronized (entries[pos]) {
            try {
                return extract.apply(entries[pos]);
            } finally {
                cleaner.accept(entries[pos]);
                capacitySemaphore.release();
            }
        }
    }


    <V> V poll(Function<E, V> extract, long timeout, TimeUnit unit) {
        try {
            if (notEmptySemaphore.tryAcquire(timeout, unit)) {
                int pos = (int) (tailCursor.getAndIncrement() % entries.length);
                synchronized (entries[pos]) {
                    try {
                        return extract.apply(entries[pos]);
                    } finally {
                        cleaner.accept(entries[pos]);
                        capacitySemaphore.release();
                    }
                }
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    <V> V poll(Function<E, V> extract) {
        if (notEmptySemaphore.tryAcquire()) {
            int pos = (int) (tailCursor.getAndIncrement() % entries.length);
            synchronized (entries[pos]) {
                try {
                    return extract.apply(entries[pos]);
                } finally {
                    cleaner.accept(entries[pos]);
                    capacitySemaphore.release();
                }
            }
        } else {
            return null;
        }
    }

    <V> V peek(Function<E, V> extract) {
        if (notEmptySemaphore.availablePermits() > 0) {
            int pos = (int) (tailCursor.get() % entries.length);
            synchronized (entries[pos]) {
                return extract.apply(entries[pos]);
            }
        } else {
            return null;
        }
    }

    int size() {
        return (int) (headCursor.get() - tailCursor.get());
    }

    <V> Iterable<V> iterator(Function<E, V> extractor) {
        return () -> new ElementIterator<>(extractor);
    }

    private class ElementIterator<V> implements Iterator<V> {
        private long i = tailCursor.get();
        private final Function<E, V> extractor;
        ElementIterator(Function<E, V> extractor) {
            this.extractor = extractor;
        }
        @Override
        public boolean hasNext() {
            return i < headCursor.get();
        }
        @Override
        public V next() {
            int pos = (int) (i++ % entries.length);
            synchronized (entries[pos]) {
                return extractor.apply(entries[pos]);
            }
        }
    }

    boolean isEmpty() {
        return notEmptySemaphore.availablePermits() == 0;
    }

}
