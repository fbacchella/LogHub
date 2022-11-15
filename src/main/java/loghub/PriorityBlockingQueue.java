package loghub;

import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import loghub.events.Event;
import lombok.Getter;

/**
 * A Queue implementation that support queuing {@link Event}. It handles internally two queue.
 * The first, with lower priority, is for blocking receivers, that can use backpressure to control bandwidth.
 * The other is for asynchronous receiver that can't control flow speed ; so this queue must be processed with higher priority that the previous one to avoid losing events.
 * To avoid starving the first queue, the wait time of each event is compared, with a weight ratio for the second queue and the longest waiting event is chosen.
 */
public class PriorityBlockingQueue extends AbstractQueue<Event>
        implements BlockingQueue<Event> {

    /**
     * Element wrapper required by the queue.
     * <p>
     * This wrapper keeps track the age of a queue element.
     */
    private static class QueueElement {

        private final Event event;
        private final long baseTime;

        /**
         * Create an {@link Event} wrapper.
         *
         * @param {@link Event}
         * 
         * @throws IllegalArgumentException if the element is {@code null}.
         */
        public QueueElement(Event event) {
            if (event == null) {
                throw new NullPointerException("Null event inserted");
            }
            this.event = event;
            this.baseTime = System.nanoTime();
        }

    }

    private final BlockingQueue<QueueElement> asyncQueue;
    private final BlockingQueue<QueueElement> syncQueue;
    private final ReadWriteLock masterlock = new ReentrantReadWriteLock();
    private final Lock readLock = masterlock.readLock();
    private final Lock writeLock = masterlock.readLock();
    private final Lock selectLock = new ReentrantLock();
    
    // A lock that will prevent ingestions of new events in receivers
    // The write lock can be held when a generic blocked sitation is detected
    private final ReadWriteLock backpressureLock = new ReentrantReadWriteLock();
    private final Lock backpressureReadLock = backpressureLock.readLock();
    private final Lock backpressureWriteLock = backpressureLock.readLock();
    // The injection thread that will process asynchronously injected events.
    private final ExecutorService asyncInjectors = Executors.newSingleThreadExecutor(r -> ThreadBuilder.get().setName("AsyncInternalInjector").setDaemon(false).build());

    @Getter
    private final int weight;

    /**
     * Creates a {@code PriorityBlockingQueue} with the given (fixed) capacity.
     *
     * @param capacity the capacity of this queue
     * @param weight the relative weight of the non blocking {@link Event}. If 0, it will disable priority management and only one queue will be used
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
        asyncQueue = new LinkedBlockingQueue<>(capacity);
        if (weight == 0) {
            syncQueue = asyncQueue;
        } else {
            syncQueue = new LinkedBlockingQueue<>(capacity);
        }
        this.weight = weight;
    }

    /**
     * Creates a {@code PriorityBlockingQueue} with a capacity of
     * {@link Integer#MAX_VALUE} and no priority management.
     */
    public PriorityBlockingQueue() {
        asyncQueue = new LinkedBlockingQueue<>();
        syncQueue = asyncQueue;
        this.weight = 0;
    }

    /**
     * Return the number of {@link Event} in the queue.
     *
     * @return the number of {@link Event} in the queue.
     */
    @Override
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
     * @return
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
                // InterruptedException can happens before or after locking
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
     * @param ev
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
     * Inserts the {@link Event} into the high priority queue if it is possible to do
     * so immediately without violating capacity restrictions, returning
     * {@code true} upon success and throwing an
     * {@link IllegalStateException} if no space is currently available.
     * When using a capacity-restricted queue, it is generally preferable to
     * use {@link #offer(Object) offer}.
     *
     * @param e the {@link Event} to add
     * @return {@code true} (as specified by {@link Collection#add})
     * @throws IllegalStateException if the element cannot be added at this
     *         time due to capacity restrictions
     * @throws NullPointerException if the specified element is {@code null}
     */
    @Override
    public boolean add(Event e) {
        readLock.lock();
        try {
            return asyncQueue.add(new QueueElement(e));
        } finally {
            readLock.unlock();
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
    @Override
    public Event peek() {
        readLock.lock();
        try {
            return resolve(BlockingQueue::peek);
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
    @Override
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

    private void privatePut(Event e, BlockingQueue<QueueElement> queue) throws InterruptedException {
        if (this.weight == 0) {
            queue.put(new QueueElement(e));
        } else {
            QueueElement qe = new QueueElement(e);
            boolean inserted = false;
            while (!inserted) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    inserted = queue.offer(qe, 10, TimeUnit.MILLISECONDS);
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
     * available. When using a capacity-restricted queue, this method is
     * generally preferable to {@link #add}, which can fail to insert an
     * element only by throwing an exception.
     *
     * @param event the {@link Event} to add
     * @return {@code true} if the {@link Event} was added to this queue, else
     *         {@code false}
     * @throws NullPointerException if the specified element is {@code null}
     */
    @Override
    public boolean offer(Event event) {
        readLock.lock();
        try {
            return asyncQueue.offer(new QueueElement(event));
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
    @Override
    public boolean offer(Event e, long timeout, TimeUnit unit) throws InterruptedException {
        return privateOffer(e, timeout, unit, asyncQueue);
    }

    /**
     * Insert the {@link Event} into the blocking queue, waiting up to the
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
    public boolean offerBlocking(Event e, long timeout, TimeUnit unit) throws InterruptedException {
        return privateOffer(e, timeout, unit, syncQueue);
    }

    private boolean privateOffer(Event e, long timeout, TimeUnit unit, BlockingQueue<QueueElement> queue) throws InterruptedException {
        if (weight == 0) {
            return queue.offer(new QueueElement(e), timeout, unit);
        } else {
            QueueElement qe = new QueueElement(e);
            long endDelay = TimeUnit.NANOSECONDS.convert(timeout, unit) + System.nanoTime();
            boolean inserted = false;
            while (!inserted && endDelay > System.nanoTime()) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    inserted = queue.offer(qe, 10, TimeUnit.MILLISECONDS);
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
    @Override
    public Event take() throws InterruptedException {
        if (weight == 0) {
            return Optional.ofNullable(asyncQueue.take()).map(qe -> qe.event).orElse(null);
        } else {
            Event found = null;
            while (found == null) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    found = resolveInterrupted(q -> q.poll(10, TimeUnit.MILLISECONDS));
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
    @Override
    public Event poll() {
        readLock.lock();
        try {
            return resolve(BlockingQueue::poll);
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
    @Override
    public Event poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (weight == 0) {
            return Optional.ofNullable(asyncQueue.poll(timeout, unit)).map(qe -> qe.event).orElse(null);
        } else {
            long endDelay = TimeUnit.NANOSECONDS.convert(timeout, unit) +  System.nanoTime();
            Event found = null;
            while (found == null && endDelay > System.nanoTime()) {
                // A loop to avoid holding the read lock.
                readLock.lockInterruptibly();
                try {
                    found = resolveInterrupted(q -> q.poll(10, TimeUnit.MILLISECONDS));
                } finally {
                    readLock.unlock();
                } 
            }
            return found;
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
    @Override
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
     * Not implemented, will throws a {@link UnsupportedOperationException}
     */
    @Override
    public int drainTo(Collection<? super Event> c) {
        throw new UnsupportedOperationException();
    }

    /**
     * Not implemented, will throws a {@link UnsupportedOperationException}
     */
    @Override
    public int drainTo(Collection<? super Event> c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    /**
     * Atomically removes all of the elements from both queues.
     * The queue will be empty after this call returns.
     */
    @Override
    public synchronized void clear() {
        writeLock.lock();
        try {
            syncQueue.clear();
            asyncQueue.clear();
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
    @Override
    public Iterator<Event> iterator() {
        if (weight == 0) {
            return asyncQueue.stream().map(qe -> qe.event).iterator();
        } else {
            // For faster lock, ensure that the ArrayList will probably be big enough
            List<QueueElement> blockingEvents = new ArrayList<>(asyncQueue.size() + 10);
            List<QueueElement> asyncEvents = new ArrayList<>(syncQueue.size() + 10);
            AtomicInteger cursorAsync = new AtomicInteger(0);
            AtomicInteger cursorBlocking = new AtomicInteger(0);
            long referenceTime = System.nanoTime();
            writeLock.lock();
            try {
                asyncEvents.addAll(asyncQueue);
                blockingEvents.addAll(syncQueue);
            } finally {
                writeLock.unlock();
            }
            return new Iterator<Event>() {

                @Override
                public boolean hasNext() {
                    return (cursorAsync.get() < asyncEvents.size()) || (cursorBlocking.get() < blockingEvents.size());
                }

                @Override
                public Event next() {
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
            };
        }
    }

    private synchronized BlockingQueue<QueueElement> select() {
        if (weight == 0) {
            return asyncQueue;
        } else {
            readLock.lock();
            selectLock.lock();
            try {
                long referenceTime = System.nanoTime();
                long syncElementDelay = Optional.ofNullable(syncQueue.peek()).map(e -> (referenceTime - e.baseTime)).orElse(-1L);
                long asyncElementDelay = Optional.ofNullable(asyncQueue.peek()).map(e -> (referenceTime - e.baseTime) * weight).orElse(-1L);
                return syncElementDelay > asyncElementDelay ? syncQueue : asyncQueue;
            } finally {
                selectLock.unlock();
                readLock.unlock();
            }
        }
    }

    private Event resolveInterrupted(FunctionInterrupted resolver) throws InterruptedException {
        BlockingQueue<QueueElement> q = select();
        return Optional.ofNullable(resolver.apply(q)).map(qe -> qe.event).orElse(null);
    }

    private Event resolve(Function<BlockingQueue<QueueElement>, QueueElement> resolver) {
        BlockingQueue<QueueElement> q = select();
        return Optional.ofNullable(resolver.apply(q)).map(qe -> qe.event).orElse(null);
    }

    private interface FunctionInterrupted {
        QueueElement apply(BlockingQueue<QueueElement> queue) throws InterruptedException ;
    }

}
