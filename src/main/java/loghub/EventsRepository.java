package loghub;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import loghub.configuration.Properties;

public class EventsRepository<KEY> {

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
    private static final HashedWheelTimer processTimeout = new HashedWheelTimer(tf);

    private final Map<KEY, PausedEvent<KEY>> pausestack = new ConcurrentHashMap<>();
    private final Map<KEY, Timeout> waiting = new ConcurrentHashMap<>();
    private final BlockingQueue<Event> mainQueue;
    private final Map<String, Pipeline> pipelines;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
        pipelines = properties.namedPipeLine;
    }

    public void pause(PausedEvent<KEY> paused) {
        PausedEvent<KEY> realpaused = paused.setRepository(this);
        pausestack.put(realpaused.key, realpaused);
        Properties.metrics.counter("paused").inc();
        if (realpaused.duration > 0 && realpaused.unit != null) {
            waiting.put(realpaused.key, processTimeout.newTimeout(i -> this.timeout(realpaused.key), realpaused.duration, realpaused.unit));
        }
    }

    public PausedEvent<KEY> cancel(KEY key) {
        PausedEvent<KEY> pe = pausestack.remove(key);
        if (pe != null) {
            Properties.metrics.counter("paused").dec();
        } else {
            logger.warn("removed illegal event with key {}", key);
        }
        Timeout task = waiting.remove(key);
        if(task != null) {
            task.cancel();
        } 
        return pe;
    }

    public boolean succed(KEY key) {
        return awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public boolean failed(KEY key) {
        return awake(key, i -> i.onFailure, i -> i.failureTransform);
    }

    public boolean timeout(KEY key) {
        return awake(key, i -> i.onTimeout, i -> i.timeoutTransform);
    }

    public boolean exception(KEY key) {
        return awake(key, i -> i.onException, i -> i.exceptionTransform);
    }

    private boolean awake(KEY key, Function<PausedEvent<KEY>, Processor> source, Function<PausedEvent<KEY>, Function<Event, Event>> transform) {
        PausedEvent<KEY> pe = pausestack.remove(key);
        Timeout task = waiting.remove(key);
        if (task  != null) {
            task.cancel();
        }
        if (pe == null) {
            return true;
        }
        Properties.metrics.counter("paused").dec();
        logger.trace("Waking up event {}", pe.event);
        pe.event.insertProcessor(source.apply(pe));
        return transform.apply(pe).apply(pe.event).inject(pipelines.get(pe.pipeline), mainQueue);
    }

    public Event get(KEY key) {
        return pausestack.get(key).event;
    }

    /**
     * Return a paused event given a key or create it using the creator function.
     * @param key the index for the new event
     * @param creator return an new paused event, called only if don't already exists
     * @return
     */
    public PausedEvent<KEY> getOrPause(KEY key, Supplier<PausedEvent<KEY>> creator) {
        return pausestack.computeIfAbsent(key, i -> {
            final PausedEvent<KEY> paused = creator.get().setRepository(this).setKey(i);
            if (paused.duration > 0 && paused.unit != null) {
                waiting.put(key, processTimeout.newTimeout(j -> this.timeout(key), paused.duration, paused.unit));
            }
            Properties.metrics.counter("paused").inc();
            return paused;
        });
    }

    public int waiting() {
        return pausestack.size();
    }

    @Override
    public String toString() {
        return "EventsRepository [" + pausestack + "]";
    }

}
