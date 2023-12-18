package loghub.queue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import loghub.ThreadBuilder;
import loghub.events.Event;
import lombok.Getter;

/**
 * A Queue implementation that support queuing {@link Event}. It handles internally two queue.
 * The first, with lower priority, is for blocking receivers, that can use backpressure to control bandwidth.
 * The other is for asynchronous receiver that can't control flow speed ; so this queue must be processed with higher priority that the previous one to avoid losing events.
 * To avoid starving the first queue, the wait time of each event is compared, with a weight ratio for the second queue and the longest waiting event is chosen.
 */
public class PriorityBlockingQueue {

    private class EventsIterator implements Iterator<Event> {
        private final List<QueueElement> blockingEvents;
        private final List<QueueElement> asyncEvents;
        private final AtomicInteger cursorAsync;
        private final AtomicInteger cursorBlocking;
        private final long referenceTime;
        EventsIterator() {
            PriorityBlockingQueue.this.writeLock.lock();
            blockingEvents = new ArrayList<>(PriorityBlockingQueue.this.asyncQueue.size() + PriorityBlockingQueue.this.asyncQueue.size() + 10);
            asyncEvents = new ArrayList<>(PriorityBlockingQueue.this.syncQueue.size() + 10);
            cursorAsync = new AtomicInteger(0);
            cursorBlocking = new AtomicInteger(0);
            referenceTime = System.nanoTime();
            try {
                for (QueueElement qe : PriorityBlockingQueue.this.asyncQueue.iterator(QueueElement::new)) {
                    asyncEvents.add(qe);
                }
                for (QueueElement qe : PriorityBlockingQueue.this.syncQueue.iterator(QueueElement::new)) {
                    blockingEvents.add(qe);
                }
            } finally {
                PriorityBlockingQueue.this.writeLock.unlock();
            }
        }

        @Override
        public boolean hasNext() {
            return (cursorAsync.get() < asyncEvents.size()) || (cursorBlocking.get() < blockingEvents.size());
        }

        @Override
        public loghub.events.Event next() {
            if ((cursorBlocking.get() != blockingEvents.size()) && cursorAsync.get() < asyncEvents.size()) {
                long blockingDelay = referenceTime - blockingEvents.get(cursorBlocking.get()).baseTime;
                long asyncDelay = (referenceTime - asyncEvents.get(cursorAsync.get()).baseTime) * weight;
                return blockingDelay > asyncDelay ?
                               blockingEvents.get(cursorBlocking.getAndIncrement()).event : asyncEvents.get(cursorAsync.getAndIncrement()).event;
            } else if (cursorBlocking.get() < blockingEvents.size()){
                return blockingEvents.get(cursorBlocking.getAndIncrement()).event;
            } else if (cursorAsync.get() < asyncEvents.size()){
                return asyncEvents.get(cursorAsync.getAndIncrement()).event;
            } else {
                throw new NoSuchElementException();
            }
        }
    }
    /**
     * Element wrapper required by the queue.
     * <p>
     * This wrapper keeps track the age of a queue element.
     */
    private static class QueueElement {

        private volatile Event event;
        private volatile long baseTime;

        public QueueElement() {
        }

        public QueueElement(QueueElement element) {
            this.event = element.event;
            this.baseTime = element.baseTime;
        }

        public void setEvent(Event event) {
            this.event = event;
            baseTime = System.nanoTime();
        }
    }

    private final RingBuffer<QueueElement> asyncQueue;
    private final RingBuffer<QueueElement> syncQueue;
    private final ReadWriteLock masterlock = new ReentrantReadWriteLock();
    private final Lock readLock = masterlock.readLock();
    private final Lock writeLock = masterlock.readLock();

    // A lock that will prevent ingestions of new events in receivers
    // The write lock can be held when a generic blocked situation is detected
    private final ReadWriteLock backpressureLock = new ReentrantReadWriteLock();
    private final Lock backpressureReadLock = backpressureLock.readLock();
    private final Lock backpressureWriteLock = backpressureLock.writeLock();
    // The injection thread that will process asynchronously injected events.
    private final ExecutorService asyncInjectors = Executors.newSingleThreadExecutor(this::getExecutorThread);

    @Getter
    private final int weight;

    /**
     * Creates a {@code PriorityBlockingQueue} with the given (fixed) capacity.
     *
     * @param capacity the capacity of this queue
     * @param weight the relative weight of the non-blocking {@link Event}. If 0, it will disable priority management and only one queue will be used
     * @throws IllegalArgumentException if {@code capacity} is not greater
     *         than zero or the weight is negative.
     */
    public PriorityBlockingQueue(int capacity, int weight) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be superior to 0");
        }
        if (weight < 0) {
            throw new IllegalArgumentException("Weight can't be negative");
        }
        asyncQueue = new RingBuffer<>(capacity, QueueElement.class, QueueElement::new, this::cleanElement);
        if (weight == 0) {
            syncQueue = asyncQueue;
        } else {
            syncQueue = new RingBuffer<>(capacity, QueueElement.class, QueueElement::new, this::cleanElement);
        }
        this.weight = weight;
    }

    /**
     * Creates a {@code PriorityBlockingQueue} with a capacity of
     * {@link Integer#MAX_VALUE} and no priority management.
     */
    public PriorityBlockingQueue() {
        asyncQueue = new RingBuffer<>(1000, QueueElement.class, QueueElement::new, this::cleanElement);
        syncQueue = asyncQueue;
        this.weight = 0;
    }

    private void cleanElement(QueueElement element) {
        element.event = null;
        element.baseTime = Long.MIN_VALUE;
    }

    /**
     * Return the number of {@link Event} in the queue.
     *
     * @return the number of {@link Event} in the queue.
     */
    public int size() {
        writeLock.lock();
        try {
            return asyncQueue.size() + (weight == 0 ? 0 : syncQueue.size());
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * <p>A method that will synchronously inject an event in the processing pipeline.</p>
     * <p>It should be used only by receivers, because it hangs or drop events if asynchronous events are waiting
     * to be injected<p>
     * <p>If blocking is set to false, it fails if there is no capacity available. However it hangs until
     * space is available.</p>
     * @param ev The event to inject.
     * @param blocking fails or block when queue is full.
     * @return true if the event was queued
     */
    public boolean inject(Event ev, boolean blocking) {
        if (blocking) {
            boolean locked = false;
            try {
                backpressureReadLock.lockInterruptibly();
                locked = true;
                putBlocking(ev);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                // InterruptedException can happen before or after locking
                // as it's thrown from lockInterruptibly or putBlocking.
                if (locked) {
                    backpressureReadLock.unlock();
                }
            }
        } else {
            if (backpressureReadLock.tryLock()) {
                try {
                    return offer(ev);
                } finally {
                    backpressureReadLock.unlock();
                }
            } else {
                return false;
            }
        }
    }

    /**
     * <p>A method that will asynchronously inject an event in the processing pipeline.</p>
     * <p>It should be called only by processors. If the queue is full, the injection thread
     * will hangs receivers using {@link #inject(Event, boolean)} to
     * allow the pipeline to process waiting events.</p>
     * @param ev the event to inject
     */
    public void asyncInject(Event ev) {
        // If offer success, no need to delay injection in the injection thread
        if (! offer(ev)) {
            asyncInjectors.execute(() -> internalAsyncAdd(ev));
        }
    }

    private void internalAsyncAdd(Event e) {
        // If offer fails, hold the write lock, so no more events collected from receiver
        // until the queue has capacity again
        if (! offer(e)) {
            try {
                backpressureWriteLock.lock();
                put(e);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            } finally {
                backpressureWriteLock.unlock();
            }
        }
    }

     /**
     * Retrieves, but does not remove, the longest waiting {@link Event} of the two queue
     * or returns {@code null} if both queues are empty.
     * <p>
     * For performance consideration, this method is not exact.
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    public Optional<Event> peek() {
        readLock.lock();
        try {
            return resolve(rb -> rb.peek(qe -> qe.event));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Inserts the specified {@link Event} into the high priority queue, waiting if necessary
     * for space to become available.
     *
     * @param e the {@link Event} to add
     * @throws InterruptedException if interrupted while waiting
     * @throws NullPointerException if the specified element is {@code null}
     */
    public void put(Event e) throws InterruptedException {
        privatePut(e, asyncQueue);
    }

    /**
     * Inserts the specified {@link Event} into the blocking queue, waiting if necessary
     * for space to become available.
     *
     * @param e the {@link Event} to add
     * @throws InterruptedException if interrupted while waiting
     * @throws NullPointerException if the specified element is {@code null}
     */
    public void putBlocking(Event e) throws InterruptedException {
        privatePut(e, syncQueue);
    }

    private void privatePut(Event e, RingBuffer<QueueElement> queue) throws InterruptedException {
        Objects.requireNonNull(e);
        Consumer<QueueElement> setter = qe -> qe.setEvent(e);
        if (this.weight == 0) {
            queue.put(setter);
        } else {
            boolean inserted = false;
            while (!inserted) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    inserted = queue.put(setter, 10, TimeUnit.MILLISECONDS);
                } finally {
                    readLock.unlock();
                } 
            }
        }
    }

    /**
     * Inserts the {@link Event} into the high priority queue if it is possible to do
     * so immediately without violating capacity restrictions, returning
     * {@code true} upon success and {@code false} if no space is currently
     * available.
     *
     * @param event the {@link Event} to add
     * @return {@code true} if the {@link Event} was added to this queue, else
     *         {@code false}
     * @throws NullPointerException if the specified element is {@code null}
     */
    public boolean offer(Event event) {
        readLock.lock();
        try {
            return asyncQueue.put(qe -> qe.setEvent(event));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Insert the {@link Event} into the asynchronous queue, waiting up to the
     * specified wait time if necessary for space to become available.
     *
     * @param e the {@link Event} to add
     * @param timeout how long to wait before giving up, in units of
     *        <code>unit</code>
     * @param unit a <code>TimeUnit</code> determining how to interpret the
     *        <code>timeout</code> parameter
     * @return <code>true</code> if successful, or <code>false</code> if
     *         the specified waiting time elapses before space is available
     * @throws InterruptedException if interrupted while waiting
     * @throws NullPointerException if the specified element is {@code null}
     */
    public boolean offer(Event e, long timeout, TimeUnit unit) throws InterruptedException {
        if (weight == 0) {
            return asyncQueue.put(qe -> qe.setEvent(e), timeout, unit);
        } else {
            long endDelay = TimeUnit.NANOSECONDS.convert(timeout, unit) + System.nanoTime();
            boolean inserted = false;
            while (!inserted && endDelay > System.nanoTime()) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    inserted = asyncQueue.put(qe -> qe.setEvent(e), 10, TimeUnit.MILLISECONDS);
                } finally {
                    readLock.unlock();
                }
            }
            return inserted;
        }
    }

    /**
     * Retrieves and removes the oldest {@link Event} using the weight ratio, waiting if necessary
     * until one becomes available.
     * <p>
     * This implementation has a delay of up to 10ms when both queues are empties to detect a new element
     * is available.
     *
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting
     */
    public Event take() throws InterruptedException {
        if (weight == 0) {
            return asyncQueue.take(qe -> qe.event);
        } else {
            Event found = null;
            while (found == null) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    found = resolveInterrupted(q -> q.poll(qe -> qe.event, 10, TimeUnit.MILLISECONDS));
                } finally {
                    readLock.unlock();
                }
            }
            return found;
        }
    }

    /**
     * Retrieves and removes the oldest {@link Event} using the weight ratio,
     * or returns {@code null} if no {@link Event} is waiting.
     *
     * @return the head of this queue, or {@code null} if no {@link Event} is waiting
     */
    public Optional<Event> poll() {
        readLock.lock();
        try {
            return resolve(rb -> rb.poll(qe -> qe.event));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Retrieves and removes the oldest {@link Event} using the weight ratio, waiting up to the
     * specified wait time if necessary for one to become available.
     *
     * <p>
     * This implementation has a delay of up to 10ms when both queues are empties to detect a new element
     * is available.
     *
     * @param timeout how long to wait before giving up, in units of
     *        {@code unit}
     * @param unit a {@code TimeUnit} determining how to interpret the
     *        {@code timeout} parameter
     * @return the head of this queue, or {@code null} if the
     *         specified waiting time elapses before an element is available
     * @throws InterruptedException if interrupted while waiting
     */
    public Optional<Event> poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (weight == 0) {
            return Optional.ofNullable(asyncQueue.poll(qe -> qe.event, timeout, unit));
        } else {
            long endDelay = TimeUnit.NANOSECONDS.convert(timeout, unit) +  System.nanoTime();
            Event found = null;
            while (found == null && endDelay > System.nanoTime()) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    found = resolveInterrupted(q -> q.poll(qe -> qe.event, 10, TimeUnit.MILLISECONDS));
                } finally {
                    readLock.unlock();
                } 
            }
            return Optional.ofNullable(found);
        }
    }

    /**
     * Returns the number of additional {@link Event} that the asynchronous queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking, or {@code Integer.MAX_VALUE} if there is no intrinsic
     * limit.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting {@code remainingCapacity}
     * because it may be the case that another thread is about to
     * insert or remove an element.
     *
     * @return the remaining capacity of the asynchronous queue
     */
    public int remainingCapacity() {
        return asyncQueue.remainingCapacity();
    }

   /**
    * Returns the number of additional {@link Event} that the blocking queue can ideally
    * (in the absence of memory or resource constraints) accept without
    * blocking, or {@code Integer.MAX_VALUE} if there is no intrinsic
    * limit.
    *
    * <p>Note that you <em>cannot</em> always tell if an attempt to insert
    * an element will succeed by inspecting {@code remainingCapacity}
    * because it may be the case that another thread is about to
    * insert or remove an element.
    *
    * @return the remaining capacity
    */
    public int remainingBlockingCapacity() {
        return syncQueue.remainingCapacity();
    }

    /**
     * Atomically removes all the elements from both queues.
     * The queue will be empty after this call returns.
     */
    public void clear() {
        writeLock.lock();
        try {
            syncQueue.clear();
            asyncQueue.clear();
        } finally {
            writeLock.unlock();
        }
    }

    public Stream<Event> stream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                        iterator(),
                        Spliterator.ORDERED)
                , false);
    }

    public Event[] toArray() {
        return stream().toArray(Event[]::new);
    }

    public boolean isEmpty() {
        if (weight == 0) {
            return asyncQueue.isEmpty();
        }
        writeLock.lock();
        try {
            return asyncQueue.isEmpty() && syncQueue.isEmpty();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns an iterator over the {@link Event} in this queue sorted by wait time, using the weight ratio.
     * The elements will be returned in order from first (head) to last (tail).
     *
     * @return an iterator over the {@link Event} in this queue in proper sequence
     */
    public Iterator<Event> iterator() {
        if (weight == 0) {
            writeLock.lock();
            List<Event> events;
            try {
                events = new ArrayList<>(asyncQueue.size() + 10);
                for (Event event: asyncQueue.iterator(qe -> qe.event)) {
                    events.add(event);
            }
            } finally {
                writeLock.unlock();
            }
            return events.iterator();
        } else {
            return new EventsIterator();
        }
    }

    private RingBuffer<QueueElement> select() {
        if (weight == 0) {
            return asyncQueue;
        } else {
            readLock.lock();
            try {
                long referenceTime = System.nanoTime();
                long syncElementDelay = Optional.ofNullable(syncQueue.peek(e -> referenceTime - e.baseTime)).orElse(-1L);
                long asyncElementDelay = Optional.ofNullable(asyncQueue.peek(e -> (referenceTime - e.baseTime) * weight)).orElse(-1L);
                return syncElementDelay > asyncElementDelay ? syncQueue : asyncQueue;
            } finally {
                readLock.unlock();
            }
        }
    }

    private Event resolveInterrupted(FunctionInterrupted resolver) throws InterruptedException {
        RingBuffer<QueueElement> q = select();
        return resolver.apply(q);
    }

    private Optional<Event> resolve(Function<RingBuffer<QueueElement>, Event> resolver) {
        RingBuffer<QueueElement> q = select();
        return Optional.ofNullable(resolver.apply(q));
    }

    private interface FunctionInterrupted {
        Event apply(RingBuffer<QueueElement> queue) throws InterruptedException ;
    }

    private Thread getExecutorThread(Runnable r) {
        return ThreadBuilder.get()
                            .setName("AsyncInternalInjector")
                            .setDaemon(false)
                            .setTask(r)
                            .build();
    }
}
