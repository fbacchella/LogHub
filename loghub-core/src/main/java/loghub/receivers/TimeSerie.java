

package loghub.receivers;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import loghub.BuilderClass;
import loghub.events.Event;
import lombok.Getter;
import lombok.Setter;

@SelfDecoder
@BuilderClass(TimeSerie.Builder.class)
public class TimeSerie extends Receiver<TimeSerie, TimeSerie.Builder> {

    private static final AtomicLong r = new AtomicLong(0);

    @Setter
    public static class Builder extends Receiver.Builder<TimeSerie, TimeSerie.Builder> {
        private double frequency = 1000.0f;
        @Override
        public TimeSerie build() {
            return new TimeSerie(this);
        }
    }
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final double frequency;

    protected TimeSerie(Builder builder) {
        super(builder);
        this.frequency = builder.frequency;
    }

    @Override
    protected Stream<Event> getStream() {
        return Stream.iterate(null,
                this::sleep,
                e -> {
                    Event event = getEventsFactory().newEvent();
                    event.put("message", Long.toString(r.getAndIncrement()));
                    return event;
                });
    }

    private boolean sleep(Event e) {
        try {
            Thread.sleep((int) (1.0f / frequency * 1000));
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return false;
        }
        return ! Thread.currentThread().isInterrupted();
    }

    @Override
    public String getReceiverName() {
        return "TimeSerie";
    }

}
