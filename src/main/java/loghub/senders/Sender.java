package loghub.senders;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;

import loghub.AbstractBuilder;
import loghub.CanBatch;
import loghub.Event;
import loghub.Filter;
import loghub.FilterException;
import loghub.Helpers;
import loghub.Stats;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.Encoder;
import lombok.Getter;
import lombok.Setter;

public abstract class Sender extends Thread implements Closeable {

    protected class Batch extends ArrayList<EventFuture> {
        private final Counter counter;
        Batch() {
            super(Sender.this.buffersize);
            counter = Properties.metrics.counter("sender." + Sender.this.getName() + ".activeBatches");
            counter.inc();
        }
        void finished() {
            super.stream().forEach(Sender.this::processStatus);
            counter.dec();
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
        protected int threads = 2;
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
    private final int buffersize;
    @Getter
    private final Filter filter;
    private volatile long lastFlush = 0;
    private final Thread[] threads;
    private final BlockingQueue<Batch> batches;
    private final Runnable publisher;
    private Batch batch;
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
            builder.threads = Math.max(1, builder.threads);
        }
        if (builder.batchSize > 0 && getClass().getAnnotation(CanBatch.class) != null) {
            isAsync = true;
            buffersize = builder.batchSize;
            threads = new Thread[builder.threads];
            batches = new ArrayBlockingQueue<>(threads.length * 2);
            publisher = getPublisher();
            batch = new Batch();
        } else {
            isAsync = getClass().getAnnotation(AsyncSender.class) != null;
            threads = null;
            buffersize = -1;
            batches = null;
            publisher = null;
        }
    }

    public boolean configure(Properties properties) {
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
     * A runnable that will be affected to threads. It consumes event and send them as bulk
     * @return
     */
    private Runnable getPublisher() {
        // Don't use a lambda as 'this' is used as a monitor
        return new Runnable() {
            @Override
            public void run() {
                try {
                    while (! Sender.this.isInterrupted() && ! closed) {
                        synchronized (this) {
                            wait();
                            logger.debug("Flush initated");
                        }
                        Batch flushedBatch;
                        while ((flushedBatch = batches.poll()) != null) {
                            Properties.metrics.histogram("sender." + getName() + ".batchesSize").update(flushedBatch.size());
                            if (flushedBatch.isEmpty()) {
                                flushedBatch.finished();
                                continue;
                            } else {
                                lastFlush = System.currentTimeMillis();
                            }
                            try (Timer.Context tctx = Properties.metrics.timer("sender." + getName() + ".flushDuration").time()) {
                                flush(flushedBatch);
                            } catch (Throwable ex) {
                                Sender.this.handleException(ex);
                                flushedBatch.forEach(fe -> fe.complete(false));
                            } finally {
                                flushedBatch.finished();
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
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
                synchronized (publisher) {
                    long now =  System.currentTimeMillis();
                    if ((now - lastFlush) > 5000) {
                        batches.add(batch);
                        batch = new Batch();
                        publisher.notify();
                    }
                }
            } catch (IllegalStateException e) {
                logger.warn("Failed to launch a scheduled batch: " + Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            }
        };
        properties.registerScheduledTask(getName() + "Flusher" , flush, 5000);
        Helpers.waitAllThreads(Arrays.stream(threads));
    }

    public void stopSending() {
        if (isWithBatch()) {
            batch.forEach(ef -> ef.complete(false));
            batches.forEach(b -> b.forEach(ef -> ef.complete(false)));
            // Notify all publisher threads that publication is finished
            synchronized (publisher) {
                publisher.notifyAll();
            }
            Arrays.stream(threads).forEach(t -> {
                try {
                    t.join(1000);
                } catch (InterruptedException e) {
                    t.interrupt();
                }
            });
        }
        closed = true;
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(new ObjectName("loghub:type=sender,servicename=" + getName() + ",name=connectionsPool"));
        } catch (InstanceNotFoundException e) {
            logger.debug("Failed to unregister mbeam: " + Helpers.resolveThrowableException(e), e);
        } catch (MalformedObjectNameException | MBeanRegistrationException e) {
            logger.error("Failed to unregister mbeam: " + Helpers.resolveThrowableException(e), e);
            logger.catching(Level.DEBUG, e);
        }
        interrupt();
    }

    protected abstract boolean send(Event e) throws SendException, EncodeException;

    protected boolean queue(Event event) {
        if (closed) {
            return false;
        }
        synchronized (publisher) {
            batch.add(event);
            if (batch.size() >= buffersize) {
                logger.debug("batch full, flush");
                try {
                    batches.put(batch);
                } catch (InterruptedException e) {
                    interrupt();
                }
                batch = new Batch();
                publisher.notify();
                if (batches.size() > threads.length) {
                    logger.warn("{} waiting flush batches, add flushing threads", () -> batches.size() - threads.length);
                }
            }
        }
        return true;
    }

    public abstract String getSenderName();

    protected void flush(Batch documents) throws SendException, EncodeException {
        throw new UnsupportedOperationException("Can't flush batch");
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
        if (filter != null) {
            try {
                return filter.filter(bs.get());
            } catch (FilterException e) {
                throw new EncodeException(e);
            }
        } else {
            return bs.get();
        }
    }

    public void run() {
        while (! isInterrupted()) {
            Event event = null;
            try {
                event = inQueue.take();
                logger.trace("New event to send: {}", event);
                boolean status = isWithBatch() ? queue(event): send(event);
                if (! isAsync) {
                    // real async or in batch mode
                    processStatus(event, status);
                } else if (isWithBatch() && ! status) {
                    // queue return false if this event was not batched
                    processStatus(event, status);
                }
                event = null;
            } catch (InterruptedException e) {
                interrupt();
                break;
            } catch (Throwable t) {
                handleException(t);
                processStatus(event, false);
            }
        }
    }

    public boolean isWithBatch() {
        return threads != null;
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
                Stats.sent.incrementAndGet();
            } else {
                String message = result.getMessage();
                if (message != null) {
                    Stats.newSenderError(message);
                } else {
                    Stats.failedSend.incrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            interrupt();
        } catch (ExecutionException e) {
            handleException(e.getCause());
        }
        result.event.end();
    }

    protected void handleException(Throwable t) {
        try {
            throw t;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (SendException e) {
            Stats.newSenderError(Helpers.resolveThrowableException(e));
            logger.error("Sending exception: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        } catch (EncodeException e) {
            Stats.newSenderError(Helpers.resolveThrowableException(e));
            logger.error("Sending exception: {}", Helpers.resolveThrowableException(e));
            logger.catching(Level.DEBUG, e);
        } catch (Error e) {
            if (Helpers.isFatal(e)) {
                throw e;
            } else {
                String message = Helpers.resolveThrowableException(e);
                Stats.newUnhandledException(e);
                logger.error("Unexpected exception: {}", message);
                logger.catching(Level.ERROR, e);
            }
        } catch (Throwable e) {
            String message = Helpers.resolveThrowableException(e);
            Stats.newUnhandledException(e);
            logger.error("Unexpected exception: {}", message);
            logger.catching(Level.ERROR, e);
        }
    }

    protected void processStatus(Event event, boolean status) {
        if (status) {
            Stats.sent.incrementAndGet();
        } else {
            Stats.failedSend.incrementAndGet();
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
