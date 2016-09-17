package loghub;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import loghub.processors.Identity;

public final class PausedEvent implements Runnable {

    private static final Processor DONOTHING = new Identity();

    public final Event event;
    public final Object key;
    public final Processor onSuccess;
    public final Processor onFailure;
    public final Processor onTimeout;
    public final TimeUnit unit;
    public final long duration;
    public final EventsRepository repository;
    public final Function<Event, Event> successTransform;
    public final Function<Event, Event> failureTransform;
    public final Function<Event, Event> timeoutTransform;

    private PausedEvent(Event event, Object key,
            Processor onSuccess, Processor onFailure, Processor onTimeout,
            Function<Event, Event> successTransform, Function<Event, Event> failureTransform, Function<Event, Event> timeoutTransform,
            long duration, TimeUnit unit,
            EventsRepository repository) {
        this.event = event;
        this.key = key;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
        this.onTimeout = onTimeout;
        this.unit = unit;
        this.duration = duration;
        this.repository = repository;
        this.successTransform = successTransform;
        this.failureTransform = failureTransform;
        this.timeoutTransform = timeoutTransform;
    }

    public PausedEvent(Event event) {
        this.event = event;
        key = event;
        onSuccess = DONOTHING;
        onFailure = DONOTHING;
        onTimeout = DONOTHING;
        unit = null;
        duration = -1;
        repository = null;
        this.successTransform = i -> i;
        this.failureTransform = i -> i;
        this.timeoutTransform = i -> i;
    }

    public final PausedEvent setKey(Object key) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onSuccess(Processor onSuccess) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onSuccess(Processor onSuccess, Function<Event, Event> successTransform) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onFailure(Processor onFailure) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onFailure(Processor onFailure, Function<Event, Event> failureTransform) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onTimeout(Processor onTimeout) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent onTimeout(Processor onTimeout, Function<Event, Event> timeoutTransform) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    final PausedEvent setRepository(EventsRepository repository) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    public final PausedEvent setTimeout(long duration, TimeUnit unit) {
        return new PausedEvent(event, key, onSuccess, onFailure, onTimeout, successTransform, failureTransform, timeoutTransform, duration, unit, repository);
    }

    /**
     * Run on event timeout
     * @see java.lang.Runnable#run()
     */
    @Override
    public final void run() {
        repository.timeout(key);
    }

}
