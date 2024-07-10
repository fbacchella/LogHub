package loghub.senders;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;

import loghub.AbstractBuilder;
import loghub.CanBatch;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Start;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.Encoder;
import loghub.events.Event;
import loghub.metrics.Stats;
import loghub.queue.RingBuffer;
import lombok.Getter;
import lombok.Setter;

public abstract class Sender extends Thread implements Closeable {

    protected static class Batch extends ArrayList<EventFuture> {
        private final Sender sender;
        Batch() {
            // Used for null batch, an always empty batch
            super(0);
            this.sender = null;
        }
        Batch(Sender sender) {
            super(sender.batchSize);
            this.sender = sender;
            Stats.newBatch(sender);
        }
        void finished() {
            if (sender != null) {
                forEach(sender::processStatus);
                Stats.doneBatch(sender);
            }
        }
        public EventFuture add(Event e) {
            if (sender != null) {
                EventFuture fe = new EventFuture(e);
                add(fe);
                return fe;
            } else {
                throw new IllegalStateException("Can't add event to the null batch");
            }
        }
        @Override
        public Stream<EventFuture> stream() {
            return super.stream().filter(EventFuture::isNotDone);
        }
    }

    // A marker to end processing
    private static final Batch NULL_BATCH = new Batch();

    public static class EventFuture extends CompletableFuture<Boolean> {
        @Getter
        private final Event event;
        @Getter
        private String message;
        public EventFuture(Event ev) {
            this.event = ev;
        }
        public EventFuture(Event ev, boolean status) {
            this.event = ev;
            this.complete(status);
        }
        public void failure(String message) {
            this.complete(false);
            this.message = message;
        }
        public void failure(Throwable t) {
            this.complete(false);
            this.message = Helpers.resolveThrowableException(t);
        }
        public boolean isNotDone() {
            return ! isDone();
        }
    }

    public abstract static class Builder<B extends Sender> extends AbstractBuilder<B> {
        @Setter
        protected Encoder encoder;
        @Setter
        protected int batchSize = -1;
        @Setter
        protected int workers = 2;
        @Setter
        protected int flushInterval = 5;
        @Setter
        private Filter filter;
        @Setter
        private ClassLoader classLoader = AbstractBuilder.class.getClassLoader();
    }

    protected final Logger logger;

    private RingBuffer<Event> inQueue;
    @Getter
    private final Encoder encoder;
    private final boolean isAsync;

    // Batch settings
    @Getter
    private final int batchSize;
    @Getter
    private final Filter filter;
    private volatile long lastFlush = 0;
    private final Thread[] threads;
    private final RingBuffer<Batch> batches;
    private final Runnable publisher;
    private final AtomicReference<Batch> batch;
    private final long flushInterval;
    private volatile boolean closed = false;
    @Getter
    private final ClassLoader classLoader;

    protected Sender(Builder<? extends Sender> builder) {
        this.classLoader = builder.classLoader;
        filter = builder.filter;
        setDaemon(true);
        setName("sender-" + getSenderName());
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        encoder = builder.encoder;
        boolean onlyBatch = Optional.ofNullable(getClass().getAnnotation(CanBatch.class)).map(CanBatch::only).orElse(false);
        if (onlyBatch) {
            builder.batchSize = Math.max(1, builder.batchSize);
            builder.workers = Math.max(1, builder.workers);
        }
        if (builder.batchSize > 0 && getClass().getAnnotation(CanBatch.class) != null) {
            flushInterval = TimeUnit.SECONDS.toMillis(builder.flushInterval);
            isAsync = true;
            batchSize = builder.batchSize;
            threads = new Thread[builder.workers];
            batches = new RingBuffer<>(threads.length * 2);
            publisher = getPublisher();
            batch = new AtomicReference<>(new Batch(this));
        } else {
            flushInterval = 0;
            isAsync = getClass().getAnnotation(AsyncSender.class) != null;
            threads = null;
            batchSize = -1;
            batches = null;
            batch = null;
            publisher = null;
        }
    }

    public boolean configure(Properties properties) {
        // Stats are reset before configure
        Stats.sendInQueueSize(this, inQueue::size);
        if (threads != null) {
            buildSyncer(properties);
        }
        if (encoder != null) {
            return encoder.configure(properties, this);
        } else if (getClass().getAnnotation(SelfEncoder.class) == null) {
            logger.error("Missing encoder");
            return false;
        } else {
            return true;
        }
    }

    /**
     * A runnable that will be affected to publishing threads. It consumes event and send them as bulk
     * 
     * @return a batch publisher runnable
     */
    protected Runnable getPublisher() {
        return this::publisher;
    }

    private void publisher() {
        try {
            while (! Thread.currentThread().isInterrupted()) {
                Batch flushedBatch = batches.take();
                if (flushedBatch == NULL_BATCH) {
                    break;
                }
                Stats.flushingBatch(this, flushedBatch.size());
                if (flushedBatch.isEmpty()) {
                    flushedBatch.finished();
                    continue;
                } else {
                    lastFlush = System.currentTimeMillis();
                }
                try (Timer.Context tctx = Stats.batchFlushTimer(this)) {
                    flush(flushedBatch);
                    flushedBatch.forEach(fe -> fe.complete(true));
                } catch (Throwable ex) {
                    handleException(ex);
                    flushedBatch.forEach(fe -> fe.complete(false));
                } finally {
                    flushedBatch.finished();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void buildSyncer(Properties properties) {
        for (int i = 0; i < threads.length ; i++) {
            threads[i] = ThreadBuilder.get().setName(getName() + "Publisher" + i)
                                            .setTask(publisher)
                                            .setDaemon(false)
                                            .build(true);
        }
        //Schedule a task to flush every 5 seconds
        properties.registerScheduledTask(getName() + "Flusher" , this::syncFlush, flushInterval);
        Helpers.waitAllThreads(Arrays.stream(threads));
    }

    private void syncFlush() {
        try {
            Batch currentBatch = batch.getAndSet(null);
            if (currentBatch != null) {
                // If null, someone is working on it, nothing to do
                long now = System.currentTimeMillis();
                if ((now - lastFlush) > flushInterval) {
                    batch.set(new Batch(this));
                    batches.put(currentBatch);
                    lastFlush = now;
                } else {
                    // Not flushing, put it back
                    batch.set(currentBatch);
                }
            }
        } catch (IllegalStateException e) {
            logger.warn("Failed to launch a scheduled batch: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        }
    }

    public final synchronized void stopSending() {
        try {
            closed = true;
            if (isWithBatch()) {
                // Stop accepting new events
                interrupt();
                // Push the current batch to the publishing batches
                Optional.ofNullable(batch.getAndSet(NULL_BATCH)).filter(b -> b != NULL_BATCH).ifPresent(b -> {
                    if (! batches.put(b, 1000, TimeUnit.MILLISECONDS)) {
                        // Failed to offer the batch, all events are failed
                        b.forEach(ef -> ef.complete(false));
                    } else {
                        logger.debug("Last batch of {} event(s)", b::size);
                    }
                });
                // Add a mark for each worker, to stop accepting
                Stream.of(threads).forEach(t -> {
                    batches.put(NULL_BATCH, 1000, TimeUnit.MILLISECONDS);
                });
                // Wait to give a chance to each worker to get a batch to process
                Stream.of(threads).forEach(t -> {
                    try {
                        t.join(1000);
                    } catch (InterruptedException e) {
                        t.interrupt();
                        Thread.currentThread().interrupt();
                    }
                });
                List<Batch> missedBatches = new ArrayList<>();
                // Empty the waiting batches list and put the end-of-processing mark instead
                batches.drainTo(missedBatches);
                if (! missedBatches.isEmpty()) {
                    missedBatches.forEach(b -> b.forEach(ef -> ef.complete(false)));
                    logger.warn("Missed {} events", () -> missedBatches.stream().flatMapToInt(b -> IntStream.of(b.size())).sum());
                }
                // Wait for all publisher threads to be finished
                Arrays.stream(threads).forEach(t -> {
                    try {
                        // Still running ? Needs to be interrupted
                        t.join(100);
                        t.interrupt();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            customStopSending();
        } finally {
            try {
                interrupt();
                join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    protected void customStopSending() {
        // Empty
    }

    protected abstract boolean send(Event e) throws SendException, EncodeException;

    protected boolean queue(Event event) {
        Batch workingBatch;
        while (true) {
            workingBatch = batch.getAndSet(null);
            if (workingBatch == null) {
                // If got a null batch, someone is already using it, run a busy loop
                Thread.yield();
            } else if (closed || workingBatch == NULL_BATCH) {
                // got the empty batch or closed, will not be processing, the sender is stopping
                batch.set(workingBatch);
                return false;
            } else {
                // got the batch, now process it
                break;
            }
        }
        try {
            workingBatch.add(event);
            if (workingBatch.size() >= batchSize) {
                // If batch is full, don't put it back, but queue it for flush
                logger.trace("Batch full, flush");
                batches.put(workingBatch);
                // A new batch will be returned to the holder
                workingBatch = new Batch(this);
                if (batches.size() > threads.length) {
                    logger.warn("{} waiting flush batches, add workers", () -> batches.size() - threads.length);
                }
            }
        } finally {
            // return the batch
            batch.set(workingBatch);
        }
        return true;
    }

    public abstract String getSenderName();

    protected void flush(Batch documents) throws SendException, EncodeException {
        throw new UnsupportedOperationException("Not a batching sender");
    }

    @FunctionalInterface
    private interface ByteSource {
        byte[] get() throws EncodeException;
    }

    protected byte[] encode(Batch documents) throws EncodeException {
        return genericEncoder(() -> encoder.encode(documents.stream().map(ef -> ef.event)));
    }

    protected byte[] encode(Event event) throws EncodeException {
        return genericEncoder(() -> encoder.encode(event));
    }

    private byte[] genericEncoder(ByteSource bs) throws EncodeException {
        byte[] encoded;
        if (filter != null) {
            try {
                encoded = filter.filter(bs.get());
            } catch (FilterException e) {
                throw new EncodeException(e);
            }
        } else {
            encoded = bs.get();
        }
        Stats.sentBytes(this, encoded.length);
        return encoded;
    }

    @Override
    public void run() {
        while (isRunning()) {
            Event event;
            try {
                event = inQueue.take();
            } catch (InterruptedException e) {
                interrupt();
                break;
            }
            try {
                logger.trace("New event to send: {}", event);
                // queue return false if this event was not batched
                boolean status = isWithBatch() ? queue(event): send(event);
                if (! isAsync || (isWithBatch() && ! status)) {
                    processStatus(event, status);
                }
            } catch (Throwable t) {
                handleException(t, event);
            }
        }
    }

    protected boolean isRunning() {
        return !closed && ! isInterrupted();
    }

    public boolean isWithBatch() {
        return threads != null;
    }

    public int getWorkers() {
        return threads != null ? threads.length : 0;
    }

    /**
     * A method that can be used inside custom {@link Sender#run()} for synchronous wait
     * 
     * @return a waiting event
     * @throws InterruptedException
     */
    protected Event getNext() throws InterruptedException {
        return inQueue.take();
    }

    protected void processStatus(EventFuture result) {
        try {
            if (Boolean.TRUE.equals(result.get())) {
                Stats.sentEvent(this);
            } else {
                Stats.failedSentEvent(this, result.getMessage(), result.event);
            }
            result.event.end();
        } catch (InterruptedException e) {
            result.event.end();
            interrupt();
        } catch (ExecutionException e) {
            handleException(e.getCause(), result.event);
        }
    }

    protected void handleException(Throwable t, Event event) {
        try {
            throw t;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SendException | EncodeException | IOException | UncheckedIOException ex) {
            Stats.failedSentEvent(this, ex, event);
            logger.error("Sending exception: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        } catch (Error ex) {
            if (Helpers.isFatal(ex)) {
                logger.fatal("Caught a fatal exception", ex);
                Start.fatalException(ex);
            } else {
                Stats.newUnhandledException(this, ex, event);
                logger.error("Unexpected exception: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.ERROR, ex);
            }
        } catch (Throwable ex) {
            Stats.newUnhandledException(this, ex, event);
            logger.error("Unexpected exception: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.ERROR, ex);
        }
        if (event != null) {
            event.end();
        }
    }

    protected void handleException(Throwable t) {
        handleException(t, null);
    }

    protected void processStatus(Event event, boolean status) {
        if (status) {
            Stats.sentEvent(this);
        } else {
            Stats.failedSentEvent(this, event);
            logger.warn("Failed {}", () -> event);
        }
        event.end();
    }

    public void setInQueue(RingBuffer<Event> inQueue) {
        this.inQueue = inQueue;
    }

    @Override
    public void close() {
        logger.debug("Closing");
        stopSending();
    }

    public final int getWorkersCount() {
        return threads != null ? threads.length : 0;
    }

    public boolean registerMbean(Object mbean, String name) {
        try {
            ObjectName on = new ObjectName("loghub:type=Senders,servicename=" + getSenderName() + ",name=" + name);
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(mbean, on);
            return true;
        } catch (NotCompliantMBeanException | MalformedObjectNameException
                 | InstanceAlreadyExistsException | MBeanRegistrationException e) {
            logger.error("jmx configuration failed: {}", Helpers.resolveThrowableException(e), e);
            return false;
        }
    }

}
