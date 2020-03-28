package loghub.senders;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

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
import loghub.Event;
import loghub.Helpers;
import loghub.Stats;
import loghub.ThreadBuilder;
import loghub.configuration.Properties;
import loghub.encoders.Encoder;
import lombok.Getter;
import lombok.Setter;

public abstract class Sender extends Thread implements Closeable {

    static class Batch extends ArrayList<Event> {
        private final Counter counter;
        Batch(Sender sender) {
            super(sender.buffersize);
            counter = Properties.metrics.counter("sender." + sender.getName() + ".activeBatches");
            counter.inc();
        }
        void finished() {
            counter.dec();
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
    };

    protected final Logger logger;

    private BlockingQueue<Event> inQueue;
    @Getter
    private final Encoder encoder;
    private final boolean isAsync;

    // Batch settings
    @Getter
    private final int buffersize;
    private volatile long lastFlush = 0;
    private final Thread[] threads;
    private final BlockingQueue<Batch> batches;
    private final Runnable publisher;
    private Batch batch;
    private volatile boolean closed = false;

    public Sender(Builder<?  extends  Sender> builder) {
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
        return () -> {
            try {
                while (!isInterrupted() && ! closed) {
                    synchronized (this) {
                        wait();
                        logger.debug("Flush initated");
                    }
                    Batch flushedBatch;
                    while ((flushedBatch = batches.poll()) != null){
                        Properties.metrics.histogram("sender." + getName() + ".batchesSize").update(flushedBatch.size());
                        if (flushedBatch.isEmpty()) {
                            flushedBatch.finished();
                            continue;
                        } else {
                            lastFlush = new Date().getTime();
                        }
                        Timer.Context tctx = Properties.metrics.timer("sender." + getName() + ".flushDuration").time();
                        try {
                            Object response = flush(flushedBatch);
                            if (response != null) {
                                logger.debug("response from http server: {}", response);
                            }
                        } catch (IOException | UncheckedIOException e) {
                            logger.error("IO exception: {}", e.getMessage());
                            logger.catching(Level.DEBUG, e);
                        } catch (Exception e) {
                            String message = Helpers.resolveThrowableException(e);
                            logger.error("Unexpected exception: {}", message);
                            logger.catching(e);
                        }
                        flushedBatch.finished();
                        tctx.close();
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
        .map(i -> i.setRunnable(publisher))
        .map(i -> i.setDaemon(false))
        .map(i -> i.build(true))
        .toArray(i -> threads);
        ;
        //Schedule a task to flush every 5 seconds
        Runnable flush = () -> {
            try {
                synchronized(publisher) {
                    long now = new Date().getTime();
                    if (( now - lastFlush) > 5000) {
                        batches.add(batch);
                        batch = new Batch(this);
                        publisher.notify();
                    }
                }
            } catch (IllegalStateException e) {
                logger.warn("Failed to launch a scheduled batch: " + Helpers.resolveThrowableException(e));
                logger.catching(Level.DEBUG, e);
            } catch (Exception e) {
                logger.error("Failed to launch a scheduled batch: " + Helpers.resolveThrowableException(e), e);
            }
        };
        properties.registerScheduledTask(getName() + "Flusher" , flush, 5000);
        Helpers.waitAllThreads(Arrays.stream(threads));
    }

    public void stopSending() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.unregisterMBean(new ObjectName("loghub:type=sender,servicename=" + getName() + ",name=connectionsPool"));
        } catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
            logger.error("Failed to unregister mbeam: " + Helpers.resolveThrowableException(e), e);
            logger.catching(Level.DEBUG, e);
        }
        interrupt();
    }

    protected abstract boolean send(Event e);

    protected boolean queue(Event event) {
        if (closed) {
            return false;
        }
        synchronized(publisher) {
            batch.add(event);
            if (batch.size() >= buffersize) {
                logger.debug("batch full, flush");
                try {
                    batches.put(batch);
                } catch (InterruptedException e) {
                    interrupt();
                }
                batch = new Batch(this);
                publisher.notify();
                if (batches.size() > threads.length) {
                    logger.warn("{} waiting flush batches, add flushing threads", () -> batches.size() - threads.length);
                }
            }
        }
        return true;
    }

    public abstract String getSenderName();

    protected Object flush(Batch documents) throws IOException {
        throw new UnsupportedOperationException("Can't send single event");
    }

    public void run() {
        while (! isInterrupted()) {
            Event event = null;
            try {
                event = inQueue.take();
                boolean status = threads == null ? send(event) : queue(event);
                if (! isAsync) {
                    processStatus(event, CompletableFuture.completedFuture(status));
                }
                event = null;
            } catch (InterruptedException e) {
                interrupt();
                break;
            } catch (Exception | StackOverflowError e) {
                CompletableFuture<Boolean> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                processStatus(event, failed);
            }
        }
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

    protected void processStatus(Event event, Future<Boolean> result) {
        try {
            if (result.get()) {
                Stats.sent.incrementAndGet();
            } else {
                Stats.failedSend.incrementAndGet();
            }
        } catch (InterruptedException e) {
            interrupt();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (Helpers.isFatal(cause)) {
                throw (Error) cause;
            }
            Stats.newProcessorException(e);
            logger.error("Send failed: {}", Helpers.resolveThrowableException(cause));
            logger.catching(Level.DEBUG, cause);
        }
        event.end();
    }

    public void setInQueue(BlockingQueue<Event> inQueue) {
        this.inQueue = inQueue;
    }

    @Override
    public void close() {
        logger.debug("Closing");
        closed = true;
        synchronized(publisher) {
            try {
                batches.put(batch);
            } catch (InterruptedException e) {
                interrupt();
            }
            batch = new Batch(this);
            publisher.notify();
        }
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

    public int getThreads() {
        return threads != null ? threads.length : 0;
    }

}
