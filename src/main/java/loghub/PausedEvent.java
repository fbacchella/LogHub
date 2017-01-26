package loghub;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import loghub.processors.Identity;

public final class PausedEvent<KEY> {

    private static final Processor DONOTHING = new Identity();

    public final Event event;
    public final KEY key;
    public final Processor onSuccess;
    public final Processor onFailure;
    public final Processor onTimeout;
    public final Processor onException;
    public final TimeUnit unit;
    public final long duration;
    public final EventsRepository<KEY> repository;
    public final Function<Event, Event> successTransform;
    public final Function<Event, Event> failureTransform;
    public final Function<Event, Event> timeoutTransform;
    public final Function<Event, Event> exceptionTransform;
    public final String pipeline;

    private PausedEvent(Event event, KEY key,
            Processor onSuccess, Processor onFailure, Processor onTimeout, Processor onException,
            Function<Event, Event> successTransform, Function<Event, Event> failureTransform, Function<Event, Event> timeoutTransform, Function<Event, Event> exceptionTransform,
            long duration, TimeUnit unit,
            EventsRepository<KEY> repository,
            String pipeline) {
        this.event = event;
        this.key = key;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
        this.onTimeout = onTimeout;
        this.onException = onException;
        this.unit = unit;
        this.duration = duration;
        this.repository = repository;
        this.successTransform = successTransform;
        this.failureTransform = failureTransform;
        this.timeoutTransform = timeoutTransform;
        this.exceptionTransform = exceptionTransform;
        this.pipeline = pipeline;
    }

    public PausedEvent(Event event, KEY key) {
        this.event = event;
        this.key = key;
        onSuccess = DONOTHING;
        onFailure = DONOTHING;
        onTimeout = DONOTHING;
        onException = DONOTHING;
        unit = null;
        duration = -1;
        repository = null;
        successTransform = i -> i;
        failureTransform = i -> i;
        timeoutTransform = i -> i;
        exceptionTransform = i -> i;
        pipeline = null;
    }

    public final PausedEvent<KEY> setKey(KEY key) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onSuccess(Processor onSuccess) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onSuccess(Processor onSuccess, Function<Event, Event> successTransform) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onFailure(Processor onFailure) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onFailure(Processor onFailure, Function<Event, Event> failureTransform) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onTimeout(Processor onTimeout) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onTimeout(Processor onTimeout, Function<Event, Event> timeoutTransform) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onException(Processor onException) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> onException(Processor onException, Function<Event, Event> exceptionTransform) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    final PausedEvent<KEY> setRepository(EventsRepository<KEY> repository) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> setTimeout(long duration, TimeUnit unit) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    public final PausedEvent<KEY> setPipeline(String pipeline) {
        return new PausedEvent<KEY>(event, key, onSuccess, onFailure, onTimeout, onException, successTransform, failureTransform, timeoutTransform, exceptionTransform, duration, unit, repository, pipeline);
    }

    @Override
    public String toString() {
        return "PausedEvent [event=" + event + ", key=" + key + "]";
    }

}
