

package loghub.receivers;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import loghub.ConnectionContext;
import loghub.Event;
import loghub.Pipeline;
import loghub.Receiver;
import loghub.configuration.Properties;

public class TimeSerie extends Receiver {

    private final static AtomicLong r = new AtomicLong(0);

    private int frequency = 1000;

    public TimeSerie(BlockingQueue<Event> outQueue, Pipeline processors) {
        super(outQueue, processors);
    }

    @Override
    public boolean configure(Properties properties) {
        decoder = Receiver.NULLDECODER;
        return super.configure(properties);
    }

    @Override
    protected Iterator<Event> getIterator() {
        final ByteBuffer buffer = ByteBuffer.allocate(8);
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
                try {
                    buffer.clear();
                    buffer.put(Long.toString(r.getAndIncrement()).getBytes());
                } catch (Exception e) {
                    throw new RuntimeException("can't push to buffer", e);
                }
                return decode(ConnectionContext.EMPTY, buffer.array());
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