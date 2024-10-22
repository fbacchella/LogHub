package loghub.senders;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;

import org.junit.Assert;

import com.codahale.metrics.Meter;

import loghub.ConnectionContext;
import loghub.configuration.Properties;
import loghub.encoders.EncodeException;
import loghub.encoders.Encoder;
import loghub.events.Event;
import loghub.events.EventsFactory;
import loghub.metrics.Stats;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SenderTools {

    private static final EventsFactory factory = new EventsFactory();

    private SenderTools() {

    }

    public static <S extends Sender> void send(Sender.Builder<S> builder)
            throws InterruptedException, EncodeException {
        ArrayBlockingQueue<Event> queue = new ArrayBlockingQueue<>(10);

        Encoder encoder = mock(Encoder.class);
        doThrow(new EncodeException("Dummy exception")).when(encoder).encode(any(Event.class));
        when(encoder.configure(any(Properties.class), any(Sender.class))).thenReturn(Boolean.TRUE);

        builder.setEncoder(encoder);
        S sender = builder.build();
        sender.setInQueue(queue);
        Assert.assertTrue(sender.configure(new Properties(Collections.emptyMap())));
        sender.start();
        Event ev = factory.newEvent(new BlockingConnectionContext());
        queue.add(ev);
        ConnectionContext<Semaphore> ctxt = ev.getConnectionContext();
        ctxt.getLocalAddress().acquire();
        Assert.assertEquals(1, Stats.getMetric(Meter.class, Sender.class, "failedSend").getCount());
        Assert.assertTrue(Stats.getSenderError().contains("Dummy exception"));
        Assert.assertEquals(1L, Stats.getFailed());
    }

}
