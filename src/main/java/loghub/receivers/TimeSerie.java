package loghub.receivers;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import loghub.Event;
import loghub.Receiver;

public class TimeSerie extends Receiver {

    private final static AtomicInteger r = new AtomicInteger(0);
    private final int rnum;

    public TimeSerie() {
        super();
        rnum = r.getAndIncrement();
    }

    @Override
    protected Iterator<Event> getIterator() {
        return new Iterator<Event>() {
            int i=0;
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Event next() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
                Event e = new Event();
                e.put("receiver", rnum);
                e.put("count", i++);
                return e;
            }

        };
    }

    @Override
    public String getReceiverName() {
        return "TimeSerie";
    }

}
