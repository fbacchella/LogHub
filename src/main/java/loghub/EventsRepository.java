package loghub;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Properties;

public class EventsRepository<KEY> {

    private final static Logger logger = LogManager.getLogger();

    private final ScheduledThreadPoolExecutor processTimeout = new ScheduledThreadPoolExecutor(0);
    private final Map<KEY, PausedEvent<KEY>> pausestack = new ConcurrentHashMap<>();
    private final BlockingQueue<Event> mainQueue;
    private final Map<String, Pipeline> pipelines;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
        pipelines = properties.namedPipeLine;
    }

    public void pause(PausedEvent<KEY> paused) {
        paused = paused.setRepository(this);
        pausestack.put(paused.key, paused);
        if (paused.duration > 0 && paused.unit != null) {
            processTimeout.schedule(paused, paused.duration, paused.unit);
        }
    }

    public PausedEvent<KEY> cancel(KEY key) {
        PausedEvent<KEY> pe = pausestack.remove(key);
        processTimeout.remove(pe);
        return pe;
    }

    public boolean succed(KEY key) {
        return awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public boolean failed(KEY key) {
        return awake(key, (i) -> i.onFailure, i -> i.failureTransform);
    }

    public boolean timeout(KEY key) {
        return awake(key, (i) -> i.onTimeout, i -> i.timeoutTransform);
    }

    public boolean exception(KEY key) {
        return awake(key, (i) -> i.onException, i -> i.exceptionTransform);
    }

    private boolean awake(KEY key, Function<PausedEvent<KEY>, Processor> source, Function<PausedEvent<KEY>, Function<Event, Event>> transform) {
        PausedEvent<KEY> pe = pausestack.remove(key);
        if (pe == null) {
            return true;
        }
        logger.trace("Waking up event {}", pe.event);
        pe.event.insertProcessor(source.apply(pe));
        processTimeout.remove(pe);
        return transform.apply(pe).apply(pe.event).inject(pipelines.get(pe.pipeline), mainQueue);
    }

    public Event get(KEY key) {
        return pausestack.get(key).event;
    }

    /**
     * Return a paused event given a key or create it using the creator function.
     * @param key the index for the new event
     * @param creator return an new paused event, called only if don't alread exists
     * @return
     */
    public PausedEvent<KEY> getOrPause(KEY key, Supplier<PausedEvent<KEY>> creator) {
        return pausestack.computeIfAbsent(key, i -> {
            PausedEvent<KEY> paused = creator.get().setRepository(this).setKey(i);
            if (paused.duration > 0 && paused.unit != null) {
                processTimeout.schedule(paused, paused.duration, paused.unit);
            }
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
