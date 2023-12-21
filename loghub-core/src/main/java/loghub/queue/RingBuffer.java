package loghub.queue;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

class RingBuffer<E> {

    private final Lock semaphoreLock = new ReentrantLock();
    private final Condition notFull  = semaphoreLock.newCondition();
    private final Condition notEmpty = semaphoreLock.newCondition();
    private final AtomicLong headCursor = new AtomicLong(0);
    private final AtomicLong tailCursor = new AtomicLong(0);
    private final E[] entries;
    private final Lock[] entryLocks;
    private final Consumer<E> cleaner;
    private final int capacityMask;


    public RingBuffer(int capacity, Class<E> entriesClass, Supplier<E> entriesSupplier, Consumer<E> cleaner) {
        // Calculate the next power of 2, greater than or equal to x.
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        capacity = 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(capacity - 1) + 5);
        capacityMask = (capacity - 1);
        entries = arrayInstance(entriesClass, capacity);
        entryLocks = new Lock[capacity];
        for (int i = 0; i < capacity; i++) {
            entries[i] = entriesSupplier.get();
            entryLocks[i] = new ReentrantLock();
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
    }

    int remainingCapacity() {
        semaphoreLock.lock();
        try {
            return entries.length - (int) (headCursor.get() - tailCursor.get());
        } finally {
            semaphoreLock.unlock();
        }
    }

    int resolvePos(Condition c1, Condition c2, BooleanSupplier check, LongSupplier cursor) throws InterruptedException {
        semaphoreLock.lock();
        try {
            while (check.getAsBoolean()) {
                c1.await();
            }
            return lockEntry(cursor.getAsLong() & capacityMask);
        } finally {
            c2.signal();
            semaphoreLock.unlock();
        }
    }

    int tryResolvePos(Condition c1, Condition c2, long timeout, TimeUnit timeUnit, BooleanSupplier check, LongSupplier cursor) throws InterruptedException {
        long end = System.nanoTime() + timeUnit.toNanos(timeout);
        if (! semaphoreLock.tryLock(timeout, timeUnit)) {
            return -1;
        }
        try {
            long now = System.nanoTime();
            while (end > now && check.getAsBoolean()) {
                c1.awaitNanos(end - now);
                now = System.nanoTime();
            }
            if (end > now) {
                return lockEntry(cursor.getAsLong() & capacityMask);
            } else {
                return -1;
            }
        } finally {
            c2.signal();
            semaphoreLock.unlock();
        }
    }

    int tryResolvePos(BooleanSupplier check, LongSupplier cursor) {
        semaphoreLock.lock();
        try {
            if (check.getAsBoolean()) {
                return lockEntry(cursor.getAsLong() & capacityMask);
            } else {
                return -1;
            }
        } finally {
            semaphoreLock.unlock();
        }
    }

    private boolean checkIsEmpty() {
        return headCursor.get() == tailCursor.get();
    }

    private boolean checkIsFull() {
        return (headCursor.get() - tailCursor.get() + 5) == (entries.length);
    }

    private int lockEntry(long pos) {
        entryLocks[(int)pos].lock();
        return (int)pos;
    }

    private void applySetter(int pos, Consumer<E> setter) {
        try {
            setter.accept(entries[pos]);
        } finally {
            entryLocks[pos].unlock();
        }
    }

    private <V> V applyExtract(int pos, Function<E, V> extract, boolean clean) {
        try {
            return extract.apply(entries[pos]);
        } finally {
            if (clean) {
                cleaner.accept(entries[pos]);
            }
            entryLocks[pos].unlock();
        }
    }

    boolean isEmpty() {
        semaphoreLock.lock();
        try {
            return checkIsEmpty();
        } finally {
            semaphoreLock.unlock();
        }
    }

    boolean put(Consumer<E> setter, long timeout, TimeUnit timeUnit) {
        try {
            int pos;
            if ((pos = tryResolvePos(notFull, notEmpty, timeout, timeUnit, this::checkIsFull, headCursor::getAndIncrement)) >= 0) {
                applySetter(pos, setter);
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
            int pos = resolvePos(notFull, notEmpty, this::checkIsFull, headCursor::getAndIncrement);
            applySetter(pos, setter);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    <V> V take(Function<E, V> extract) throws InterruptedException {
        int pos = resolvePos(notEmpty, notFull, this::checkIsEmpty, tailCursor::getAndIncrement);
        return applyExtract(pos, extract, true);
    }


    <V> V poll(Function<E, V> extract, long timeout, TimeUnit unit) {
        try {
            int pos;
            if ((pos = tryResolvePos(notEmpty, notFull, timeout, unit, this::checkIsEmpty, tailCursor::getAndIncrement)) >= 0) {
                return applyExtract(pos, extract, true);
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    <V> V poll(Function<E, V> extract) {
        int pos;
        if ((pos = tryResolvePos(this::checkIsEmpty, tailCursor::getAndIncrement)) >= 0) {
            return applyExtract(pos, extract, true);
        } else {
            return null;
        }
    }

    <V> V peek(Function<E, V> extract) {
        int pos;
        if ((pos = tryResolvePos(this::checkIsEmpty, tailCursor::get)) > 0) {
            return applyExtract(pos, extract, false);
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
            int pos = (int) (i++  & capacityMask);
            return extractor.apply(entries[pos]);
        }
    }

}
