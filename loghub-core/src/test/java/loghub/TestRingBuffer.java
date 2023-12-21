package loghub.queue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import loghub.ThreadBuilder;

public class TestRingBuffer {

    @Test(timeout = 5000)
    public void testSimple() throws InterruptedException {
        RingBuffer<AtomicInteger> rb = new RingBuffer<>(16, AtomicInteger.class, AtomicInteger::new, i -> i.set(Integer.MIN_VALUE));
        Assert.assertTrue(rb.isEmpty());
        Assert.assertEquals(null, rb.peek(AtomicInteger::get));
        for (AtomicInteger i = new AtomicInteger(0); i.get() < 156; i.incrementAndGet()) {
            Assert.assertTrue(rb.put(ai -> ai.set(i.get())));
        }
        boolean putted = rb.put( i-> i.set(Integer.MAX_VALUE), 1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(putted);
        for (int i = 0; i < 156; i++ ) {
            Integer io = rb.take(AtomicInteger::get);
            Assert.assertEquals(i, io.intValue());
        }
        Assert.assertTrue(rb.isEmpty());
    }

    @Test//(timeout = 5000)
    public void concurrent() throws InterruptedException {
        AtomicReference<Throwable> failed1 = new AtomicReference<>(null);
        AtomicReference<Throwable> failed2 = new AtomicReference<>(null);
        AtomicReference<Throwable> failed3 = new AtomicReference<>(null);
        RingBuffer<AtomicInteger> rb = new RingBuffer<>(16, AtomicInteger.class, () -> new AtomicInteger(Integer.MIN_VALUE), i -> i.set(Integer.MIN_VALUE));

        Thread t1 = ThreadBuilder.get().setName("PutterThread").setTask(() -> {
            try {
                for (AtomicInteger i = new AtomicInteger(0); i.get() < 1000; i.incrementAndGet()) {
                    Assert.assertTrue(rb.put(ai -> ai.set(i.get())));
                }
            } catch (Throwable e) {
                e.printStackTrace();
                failed2.set(e);
            }
        }).build(true);
        Thread t2 = ThreadBuilder.get().setName("TakerThread").setTask(() -> {
            try {
                for (int i = 0; i < 1000; i++ ) {
                    Integer io = rb.take(AtomicInteger::get);
                    Assert.assertEquals(i, io.intValue());
                }
            } catch (Throwable e) {
                e.printStackTrace();
                failed2.set(e);
            }
        }).build(true);
        Thread t3 = ThreadBuilder.get().setName("WatcherThread").setTask(() -> {
            try {
                for (int i = 0; i < 1000; i++ ) {
                    rb.peek(ai -> ai.get());
                    Thread.yield();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                failed3.set(e);
            }
        }).build(true);
        t1.join(1000, 0);
        t2.join(1000, 0);
        t3.join(1000, 0);
        for (AtomicReference<Throwable> art: List.of(failed1, failed2, failed3)) {
            Optional.ofNullable(art.get()).ifPresent(Throwable::printStackTrace);
            Assert.assertNull(art.get());
        }
    }

    @Test(timeout = 5000)
    public void full() throws InterruptedException {
        RingBuffer<AtomicInteger> rb = new RingBuffer<>(10, AtomicInteger.class, AtomicInteger::new, i -> i.set(Integer.MIN_VALUE));
        for (AtomicInteger i = new AtomicInteger(0); i.get() < 16; i.incrementAndGet()) {
            Assert.assertTrue(rb.put(ai -> ai.set(i.get())));
        }
        for (int i = 0; i < 16; i++ ) {
            Integer io = rb.take(ai -> ai.get());
            Assert.assertEquals(i, io.intValue());
        }
    }

}
