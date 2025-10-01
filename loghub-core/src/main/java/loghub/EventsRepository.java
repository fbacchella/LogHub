package loghub;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import loghub.configuration.Properties;
import loghub.events.Event;
import loghub.metrics.Stats;
import loghub.metrics.Stats.PipelineStat;

public class EventsRepository<KEY> {

    private static class PauseContext<K> {
        private final PausedEvent<K> pausedEvent;
        private final Timeout task;
        private final long startTime;

        private PauseContext(PausedEvent<K> pausedEvent, Timeout task) {
            this.pausedEvent = pausedEvent;
            this.task = task;
            // Its current pipeline is null, it's a new event
            if (pausedEvent.event.getCurrentPipeline() != null) {
                Stats.pauseEvent(pausedEvent.event.getCurrentPipeline());
                startTime = System.nanoTime();
            } else {
                startTime = Long.MAX_VALUE;
            }
        }

        private void restartEvent() {
            Stats.restartEvent(pausedEvent.event.getCurrentPipeline(), startTime);
        }

        static <K> PauseContext<K> of(PausedEvent<K> paused, EventsRepository<K> repository) {
            Timeout task;
            if (paused.timeoutHandling && paused.duration > 0 && paused.unit != null) {
                task = repository.processExpiration.newTimeout(i -> repository.runTimeout(paused), paused.duration, paused.unit);
            } else {
                task = null;
            }
            return new PauseContext<>(paused, task);
        }
    }

    private static final Logger logger = LogManager.getLogger();

    private final Map<KEY, PauseContext<KEY>> allPaused = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue mainQueue;
    private final ReadWriteLock running = new ReentrantReadWriteLock();
    private final HashedWheelTimer processExpiration;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
        processExpiration = properties.processExpiration;
        properties.registerEventsRepository(this);
    }

    public int stop() {
        // Never released, will never accept new event anymore
        running.writeLock().lock();
        int count;
        int tryWait = 0;
        // Wait 1 second to allows some sleeping events to wake up
        while ((count = allPaused.size()) > 0 && tryWait++ < 10) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        allPaused.keySet().forEach(
                k -> Optional.ofNullable(allPaused.remove(k))
                             .map(c -> {
                                Optional.ofNullable(c.task).ifPresent(Timeout::cancel);
                                return c.pausedEvent;
                             })
                             .ifPresent(pe -> pe.timeout(pe.event, k)));
        return count;
    }

    public boolean pause(PausedEvent<KEY> paused) {
        if (running.readLock().tryLock()) {
            try {
                logger.trace("Pausing {}", paused);
                allPaused.computeIfAbsent(paused.key, k -> PauseContext.of(paused, this));
                return true;
            } finally {
                running.readLock().unlock();
            }
        } else {
            return false;
        }
    }

    /**
     * Return a paused event given a key or create it using the creator function.
     * @param key the index for the new event
     * @param creator return an new paused event, called only if don't already exists
     * @return
     */
    public PausedEvent<KEY> getOrPause(KEY key, Function<KEY, PausedEvent<KEY>> creator) {
        logger.trace("looking for key {}", key);
        return allPaused.computeIfAbsent(key, i -> {
            PausedEvent<KEY> paused = creator.apply(i);
            return PauseContext.of(paused, this);
        }).pausedEvent;
    }

    public PausedEvent<KEY> cancel(KEY key) {
        if (running.readLock().tryLock()) {
            logger.trace("cancel {}", key);
            try {
                PauseContext<KEY> ctx = allPaused.remove(key);
                if (ctx == null) {
                    logger.warn("Canceling unknown event with key {}", key);
                    return null;
                }
                if (ctx.task != null) {
                    ctx.task.cancel();
                }
                ctx.restartEvent();
                return ctx.pausedEvent;
            } finally {
                running.readLock().unlock();
            }
        } else {
            return null;
        }
    }

    public void succed(KEY key) {
        logger.trace("succed {}", key);
        awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public void failed(KEY key) {
        logger.trace("failed {}", key);
        awake(key, i -> i.onFailure, i -> i.failureTransform);
    }

    public void timeout(KEY key) {
        logger.trace("timeout {}", key);
        Optional.ofNullable(allPaused.get(key)).map(p -> p.pausedEvent).ifPresent(pe -> pe.timeout(pe.event, key));
        awake(key, i -> i.onTimeout, i -> i.timeoutTransform);
    }

    public void exception(KEY key) {
        awake(key, i -> i.onException, i -> i.exceptionTransform);
    }

    private void awake(KEY key, Function<PausedEvent<KEY>, Processor> source, Function<PausedEvent<KEY>, Function<Event, Event>> transform) {
        PauseContext<KEY> ctx = allPaused.remove(key);
        if (ctx == null) {
            return;
        }
        if (ctx.task != null) {
            ctx.task.cancel();
        }
        ctx.restartEvent();
        PausedEvent<KEY> pausedEvent = ctx.pausedEvent;
        Event event = pausedEvent.event;
        logger.trace("Waking up event {}", event);
        // Insert the processor that will execute the state (failure, success, timeout)
        event.insertProcessor(source.apply(pausedEvent));
        // Eventually transform the event before handling it
        Event transformed = transform.apply(pausedEvent).apply(event);
        mainQueue.asyncInject(transformed);
    }

    private void runTimeout(PausedEvent<KEY> paused) {
        // HashedWheelTimer silently swallows Throwable, we handle them ourselves
        try {
            timeout(paused.key);
        } catch (Throwable ex) {
            Stats.pipelineHanding(paused.event, PipelineStat.EXCEPTION, ex);
            if (Helpers.isFatal(ex)) {
                logger.fatal("Caught a critical exception", ex);
                ShutdownTask.fatalException(ex);
            } else {
                logger.error("Async timeout handler failed: {}", Helpers.resolveThrowableException(ex));
                logger.catching(Level.DEBUG, ex);
            }
        }
    }

    public Event get(KEY key) {
        return allPaused.get(key).pausedEvent.event;
    }

    public int waiting() {
        return allPaused.size();
    }

    @Override
    public String toString() {
        return "EventsRepository [" + allPaused + "]";
    }

}
