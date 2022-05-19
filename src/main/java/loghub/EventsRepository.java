package loghub;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import loghub.configuration.Properties;
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
            // It current pipeline is null, it's new event
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
                task = processExpiration.newTimeout(i -> repository.runTimeout(paused), paused.duration, paused.unit);
            } else {
                task = null;
            }
            return new PauseContext<>(paused, task);
        }
    }

    private static final Logger logger = LogManager.getLogger();

    private static final HashedWheelTimer processExpiration;
    static {
        processExpiration = new HashedWheelTimer(ThreadBuilder.get().setDaemon(true).getFactory("EventsRepository-timeoutmanager"));
        processExpiration.start();
    }

    private final Map<KEY, PauseContext<KEY>> allPaused = new ConcurrentHashMap<>();
    private final PriorityBlockingQueue mainQueue;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
    }

    public PausedEvent<KEY> pause(PausedEvent<KEY> paused) {
        logger.trace("Pausing {}", paused);
        return allPaused.computeIfAbsent(paused.key, k -> PauseContext.of(paused, this)).pausedEvent;
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
        logger.trace("cancel {}", key);
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
    }

    public void succed(KEY key) throws InterruptedException {
        logger.trace("succed {}", key);
        awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public void failed(KEY key) throws InterruptedException {
        logger.trace("failed {}", key);
        awake(key, i -> i.onFailure, i -> i.failureTransform);
    }

    public void timeout(KEY key) throws InterruptedException {
        logger.trace("timeout {}", key);
        Optional.ofNullable(allPaused.get(key)).map(p -> p.pausedEvent).ifPresent(pe -> pe.timeout(pe.event, key));
        awake(key, i -> i.onTimeout, i -> i.timeoutTransform);
    }

    public void exception(KEY key) throws InterruptedException {
        awake(key, i -> i.onException, i -> i.exceptionTransform);
    }

    private void awake(KEY key, Function<PausedEvent<KEY>, Processor> source, Function<PausedEvent<KEY>, Function<Event, Event>> transform) throws InterruptedException {
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
            Stats.pipelineHanding(paused.event.getCurrentPipeline(), PipelineStat.EXCEPTION, ex);
            if (Helpers.isFatal(ex)) {
                logger.fatal("Caught a critical exception", ex);
                Start.fatalException(ex);
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
