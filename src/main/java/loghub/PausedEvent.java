package loghub;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import loghub.processors.Identity;
import lombok.Getter;

public class PausedEvent<KEY> {

    private static final Processor DONOTHING = new Identity();

    public final Event event;
    public final KEY key;
    public final Processor onSuccess;
    public final Processor onFailure;
    public final Processor onTimeout;
    public final Processor onException;
    public final TimeUnit unit;
    public final long duration;
    public final Function<Event, Event> successTransform;
    public final Function<Event, Event> failureTransform;
    public final Function<Event, Event> timeoutTransform;
    public final Function<Event, Event> exceptionTransform;

    /**
     * A flag that avoid dual processing that might happens with time out
     */
    @Getter
    private boolean done = false;

    private PausedEvent(Builder<KEY>  builder) {
        this.event = builder.event;
        this.key = builder.key;
        this.onSuccess = builder.onSuccess;
        this.onFailure = builder.onFailure;
        this.onTimeout = builder.onExpiration;
        this.onException = builder.onException;
        this.unit = builder.unit;
        this.duration = builder.duration;
        this.successTransform = builder.successTransform;
        this.failureTransform = builder.failureTransform;
        this.timeoutTransform = builder.expirationTransform;
        this.exceptionTransform = builder.exceptionTransform;
    }

    public void done() {
        done = true;
    }

    @Override
    public String toString() {
        return "PausedEvent [event=" + event + ", key=" + key + "]";
    }

    /**
     * Creates builder to build {@link PausedEvent}.
     * @return created builder
     */

    public static <KEY> Builder<KEY> builder(Event event, KEY key) {
        return new Builder<KEY>(event, key);
    }

    /**
     * Builder to build {@link PausedEvent}.
     */
    public static final class Builder<KEY> {
        private Event event;
        private KEY key;
        private Processor onSuccess = DONOTHING;
        private Processor onFailure = DONOTHING;
        private Processor onExpiration = DONOTHING;
        private Processor onException = DONOTHING;
        private TimeUnit unit = null;
        private long duration = -1;
        private Function<Event, Event> successTransform = i -> i;
        private Function<Event, Event> failureTransform = i -> i;
        private Function<Event, Event> expirationTransform = i -> i;
        private Function<Event, Event> exceptionTransform = i -> i;

        private Builder(Event event, KEY key) {
            this.event = event;
            this.key = key;
        }

        public Builder<KEY> expiration(long duration, TimeUnit unit) {
            this.duration = duration;
            this.unit = unit;
            return this;
        }

        public Builder<KEY> onSuccess(Processor onSuccess) {
            this.onSuccess = onSuccess;
            return this;
        }

        public Builder<KEY> onSuccess(Processor onSuccess, Function<Event, Event> successTransform) {
            this.onSuccess = onSuccess;
            this.successTransform = successTransform;
            return this;
        }

        public Builder<KEY> onFailure(Processor onFailure) {
            this.onFailure = onFailure;
            return this;
        }

        public Builder<KEY> onFailure(Processor onFailure, Function<Event, Event> failureTransform) {
            this.onFailure = onFailure;
            this.failureTransform = failureTransform;
            return this;
        }

        public Builder<KEY> onExpiration(Processor onExpiration, Function<Event, Event> expirationTransform) {
            this.onExpiration = onExpiration;
            this.expirationTransform = expirationTransform;
            return this;
        }

        public Builder<KEY> onException(Processor onException) {
            this.onException = onException;
            return this;
        }

        public Builder<KEY> onException(Processor onException, Function<Event, Event> exceptionTransform) {
            this.onException = onException;
            this.exceptionTransform = exceptionTransform;
            return this;
        }

        public PausedEvent<KEY> build() {
            return new PausedEvent<KEY>(this);
        }
    }

}
