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

public class EventsRepository {

    private final static Logger logger = LogManager.getLogger();

    private final ScheduledThreadPoolExecutor processTimeout = new ScheduledThreadPoolExecutor(0);
    private final Map<Object, PausedEvent> pausestack = new ConcurrentHashMap<>();
    private final BlockingQueue<Event> mainQueue;

    public EventsRepository(Properties properties) {
        mainQueue = properties.mainQueue;
    }

    public void pause(PausedEvent paused) {
        paused = paused.setRepository(this);
        pausestack.put(paused.key, paused);
        processTimeout.schedule(paused, paused.duration, paused.unit);
    }

    public boolean succed(Object key) {
        return awake(key, i -> i.onSuccess, i -> i.successTransform);
    }

    public boolean failed(Object key) {
        return awake(key, (i) -> i.onFailure, i -> i.failureTransform);
    }

    public boolean timeout(Object key) {
        return awake(key, (i) -> i.onTimeout, i -> i.timeoutTransform);
    }

    private boolean awake(Object key, Function<PausedEvent, Processor> source, Function<PausedEvent, Function<Event, Event>> transform) {
        PausedEvent pe = pausestack.remove(key);
        if (pe == null) {
            return true;
        }
        logger.trace("Waking up event {}", pe.event);
        pe.event.insertProcessor(source.apply(pe));
        processTimeout.remove(pe);
        return mainQueue.offer(transform.apply(pe).apply(pe.event));
    }

    public Event get(Object key) {
        return pausestack.get(key).event;
    }

    /**
     * Return a paused event given a key or create it using the creator function.
     * @param key the index for the new event
     * @param creator return an new paused event, called only if don't alread exists
     * @return
     */
    public PausedEvent getOrPause(Object key, Supplier<PausedEvent> creator) {
        return pausestack.computeIfAbsent(key, i -> {
            PausedEvent paused = creator.get().setRepository(this).setKey(i);
            if (paused.duration > 0 && paused.unit != null) {
                processTimeout.schedule(paused, paused.duration, paused.unit);
            }
            return paused;
        });
    }

    @Override
    public String toString() {
        return "EventsRepository [" + pausestack + "]";
    }

}
