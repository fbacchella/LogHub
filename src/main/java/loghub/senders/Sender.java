package loghub.senders;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Timer;

import loghub.AbstractBuilder;
import loghub.CanBatch;
import loghub.Event;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Start;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.Encoder;
import loghub.metrics.Stats;
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
            super.stream().forEach(sender::processStatus);
            Optional.ofNullable(sender).ifPresent(Stats::doneBatch);
        }
        public EventFuture add(Event e) {
            EventFuture fe = new EventFuture(e);
            add(fe);
            return fe;
        }
        @Override
        public Stream<EventFuture> stream() {
            return super.stream().filter(EventFuture::isNotDone);
        }
        @Override
        public void forEach(Consumer<? super EventFuture> action) {
            stream().forEach(action);
        }
    }

    // A marker to end processing
    static private final Batch NULLBATCH = new Batch();

    static public class EventFuture extends CompletableFuture<Boolean> {
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
    };

    protected final Logger logger;

    private BlockingQueue<Event> inQueue;
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
    private final BlockingQueue<Batch> batches;
    private final Runnable publisher;
    private final AtomicReference<Batch> batch = new AtomicReference<>();
    private final long flushInterval;
    private volatile boolean closed = false;

    public Sender(Builder<?  extends  Sender> builder) {
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
            batches = new LinkedBlockingQueue<>(threads.length * 8);
            publisher = getPublisher();
            batch.set(new Batch(this));
        } else {
            flushInterval = 0;
            isAsync = getClass().getAnnotation(AsyncSender.class) != null;
            threads = null;
            batchSize = -1;
            batches = null;
            publisher = null;
        }
    }

    public boolean configure(Properties properties) {
        // Stats is reset before configure
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
     * @return
     */
    protected Runnable getPublisher() {
        return () -> {
            try {
                while (true) {
                    Batch flushedBatch = batches.take();
                    if (flushedBatch == NULLBATCH) {
                        break;
                    }
                    Stats.updateBatchSize(this, flushedBatch.size());
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
                        Sender.this.handleException(ex);
                        flushedBatch.forEach(fe -> fe.complete(false));
                    } finally {
                        flushedBatch.finished();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

    protected void buildSyncer(Properties properties) {
        IntStream.rangeClosed(1, threads.length)
                 .mapToObj(i ->getName() + "Publisher" + i)
                 .map(i -> ThreadBuilder.get().setName(i))
                 .map(i -> i.setTask(publisher))
                 .map(i -> i.setDaemon(false))
                 .map(i -> i.build(true))
                 .toArray(i -> threads);
        //Schedule a task to flush every 5 seconds
        Runnable flush = () -> {
            try {
                long now =  System.currentTimeMillis();
                if ((now - lastFlush) > flushInterval) {
                    batches.add(batch.getAndSet(new Batch(this)));
                }
            } catch (IllegalStateException e) {
                logger.warn("Failed to launch a scheduled batch: " + Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            }
        };
        properties.registerScheduledTask(getName() + "Flusher" , flush, flushInterval);
        Helpers.waitAllThreads(Arrays.stream(threads));
    }

    public final synchronized void stopSending() {
        try {
            closed = true;
            interrupt();
            customStopSending();
            if (isWithBatch()) {
                List<Batch> missedBatches = new ArrayList<>();
                // Empty the waiting batches list and put the end-of-processing mark instead
                batches.drainTo(missedBatches);
                // Add a mark for each worker
                for (int i = 0; i < this.threads.length; i++) {
                    batches.add(NULLBATCH);
                }
                // Mark all waiting events as missed
                batch.get().forEach(ef -> ef.complete(false));
                missedBatches.forEach(b -> b.forEach(ef -> ef.complete(false)));
                // Wait for all publisher threads to be finished
                Arrays.stream(threads).forEach(t -> {
                    try {
                        t.join(1000);
                        t.interrupt();
                    } catch (InterruptedException e) {
                        interrupt();
                    }
                });
            }
        } finally {
            try {
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
        if (closed) {
            return false;
        }
        batch.get().add(event);
        if (batch.get().size() >= batchSize) {
            logger.debug("batch full, flush");
            try {
                batches.put(batch.getAndSet(new Batch(this)));
            } catch (InterruptedException e) {
                interrupt();
            }
            if (batches.size() > threads.length) {
                logger.warn("{} waiting flush batches, add workers", () -> batches.size() - threads.length);
            }
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

    public void run() {
        while (isRunning()) {
            Event event = null;
            try {
                event = inQueue.take();
            } catch (InterruptedException e) {
                interrupt();
                break;
            }
            try {
                logger.trace("New event to send: {}", event);
                boolean status = isWithBatch() ? queue(event): send(event);
                if (! isAsync) {
                    processStatus(event, status);
                } else if (isWithBatch() && ! status) {
                    // queue return false if this event was not batched
                    processStatus(event, status);
                }
                event = null;
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
            if (result.get()) {
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
        } catch (SendException | EncodeException | IOException ex) {
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
        }
        event.end();
    }

    public void setInQueue(BlockingQueue<Event> inQueue) {
        this.inQueue = inQueue;
    }

    @Override
    public void close() {
        logger.debug("Closing");
        stopSending();
    }

    public int getThreads() {
        return threads != null ? threads.length : 0;
    }

}
