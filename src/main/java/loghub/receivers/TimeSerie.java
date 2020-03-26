

package loghub.receivers;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import loghub.BuilderClass;
import loghub.ConnectionContext;
import loghub.Event;
import lombok.Getter;
import lombok.Setter;

@SelfDecoder
@BuilderClass(TimeSerie.Builder.class)
public class TimeSerie extends Receiver {

    private final static AtomicLong r = new AtomicLong(0);

    public static class Builder extends Receiver.Builder<TimeSerie> {
        @Setter
        private int frequency = 1000;
        @Override
        public TimeSerie build() {
            return new TimeSerie(this);
        }
    };
    public static Builder getBuilder() {
        return new Builder();
    }

    @Getter
    private final int frequency;

    protected TimeSerie(Builder builder) {
        super(builder);
        this.frequency = builder.frequency;
    }

    @Override
    protected Iterator<Event> getIterator() {
        return new Iterator<Event>() {
            @Override
            public boolean hasNext() {
                try {
                    Thread.sleep((int)(1.0/frequency * 1000));
                    return true;
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            @Override
            public Event next() {
                if (Thread.interrupted()) {
                    throw new NoSuchElementException();
                } else {
                    Event event = Event.emptyEvent(ConnectionContext.EMPTY);
                    event.put("message", Long.toString(r.getAndIncrement()));
                    return event;
                }
            }
        };
    }

    @Override
    public String getReceiverName() {
        return "TimeSerie";
    }

}
