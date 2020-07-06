package loghub;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
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
        private final long startTime = System.nanoTime();

        private PauseContext(PausedEvent<K> pausedEvent, Timeout task) {
            this.pausedEvent = pausedEvent;
            this.task = task;
            Stats.pauseEvent(pausedEvent.event.getCurrentPipeline());
        }

        private void restartEvent() {
            Stats.restartEvent(pausedEvent.event.getCurrentPipeline(), startTime);
        }

        static <K> PauseContext<K> of(PausedEvent<K> paused, EventsRepository<K> repository) {
            Timeout task;
            if (paused.duration > 0 && paused.unit != null) {
                task = processExpiration.newTimeout(i -> repository.runTimeout(paused), paused.duration, paused.unit);
            } else {
                task = null;
            }
            return new PauseContext<K>(paused, task);
        }
    }

    private static final Logger logger = LogManager.getLogger();

    private static final ThreadFactory tf = new ThreadFactory() {
        private final AtomicInteger counter = new AtomicInteger(0);
        private final ThreadFactory defaulttf = Executors.defaultThreadFactory();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = defaulttf.newThread(r);
            t.setName("EventsRepository-timeoutmanager-" + counter.incrementAndGet());
            return t;
        }
    };

    private static final HashedWheelTimer processExpiration = new HashedWheelTimer(tf);
    static {
        processExpiration.start();
    }

    private final Map<KEY, PauseContext<KEY>> pausestack = new ConcurrentHashMap<>();
    private final BlockingQueue<Event> mainQueue;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
    }

    public PausedEvent<KEY> pause(PausedEvent<KEY> paused) {
        logger.trace("Pausing {}", paused);
        return pausestack.computeIfAbsent(paused.key, k -> PauseContext.of(paused, this)).pausedEvent;
    }

    /**
     * Return a paused event given a key or create it using the creator function.
     * @param key the index for the new event
     * @param creator return an new paused event, called only if don't already exists
     * @return
     */
    public PausedEvent<KEY> getOrPause(KEY key, Function<KEY, PausedEvent<KEY>> creator) {
        logger.trace("looking for key {}", key);
        return pausestack.computeIfAbsent(key, i -> {
            PausedEvent<KEY> paused = creator.apply(i);
            return PauseContext.of(paused, this);
        }).pausedEvent;
    }

    public PausedEvent<KEY> cancel(KEY key) {
        PauseContext<KEY> ctx = pausestack.remove(key);
        if (ctx == null) {
            logger.warn("removed illegal event with key {}", key);
            return null;
        }
        if(ctx.task != null) {
            ctx.task.cancel();
        }
        ctx.restartEvent();
        return ctx.pausedEvent;
    }

    public boolean succed(KEY key) {
        return awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public boolean failed(KEY key) {
        return awake(key, i -> i.onFailure, i -> i.failureTransform);
    }

    public boolean timeout(KEY key) {
        Optional.ofNullable(pausestack.get(key)).map(p -> p.pausedEvent).ifPresent(PausedEvent::done);
        return awake(key, i -> i.onTimeout, i -> i.timeoutTransform);
    }

    public boolean exception(KEY key) {
        return awake(key, i -> i.onException, i -> i.exceptionTransform);
    }

    private boolean awake(KEY key, Function<PausedEvent<KEY>, Processor> source, Function<PausedEvent<KEY>, Function<Event, Event>> transform) {
        PauseContext<KEY> ctx = pausestack.remove(key);
        if (ctx == null) {
            return true;
        }
        if (ctx.task != null) {
            ctx.task.cancel();
        }
        ctx.restartEvent();
        logger.trace("Waking up event {}", ctx.pausedEvent.event);
        ctx.pausedEvent.event.insertProcessor(source.apply(ctx.pausedEvent));
        Event transformed = transform.apply(ctx.pausedEvent).apply(ctx.pausedEvent.event);
        return mainQueue.offer(transformed);
    }
    
    private void runTimeout(PausedEvent<KEY> paused) {
        // HashedWheelTimer silently swallows Throwable, we handle them ourselves
        try {
            timeout(paused.key);
        } catch (Throwable ex) {
            Stats.pipelineHanding(paused.event.getCurrentPipeline(), PipelineStat.EXCEPTION, ex);
            Level l;
            if (Helpers.isFatal(ex)) {
                ex.printStackTrace();
                l = Level.FATAL;
            } else {
                l = Level.ERROR;
            }
            logger.log(l, "Async timeout handler failed: {}", Helpers.resolveThrowableException(ex));
            logger.catching(Level.DEBUG, ex);
        }
    }

    public Event get(KEY key) {
        return pausestack.get(key).pausedEvent.event;
    }

    public int waiting() {
        return pausestack.size();
    }

    @Override
    public String toString() {
        return "EventsRepository [" + pausestack + "]";
    }

}
