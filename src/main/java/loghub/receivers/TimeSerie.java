

package loghub.receivers;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Receiver;
import loghub.SelfDecoder;

@SelfDecoder
public class TimeSerie extends Receiver {

    private final static AtomicLong r = new AtomicLong(0);

    private int frequency = 1000;

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
                Event event = Event.emptyEvent(ConnectionContext.EMPTY);
                event.put("message", Long.toString(r.getAndIncrement()));
                return event;
            }
        };
    }

    @Override
    public String getReceiverName() {
        return "TimeSerie";
    }

    /**
     * @return the interval
     */
    public int getFrequency() {
        return frequency;
    }

    /**
     * @param frequency the interval to set
     */
    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

}