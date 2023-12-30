package loghub.queue;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import loghub.ThreadBuilder;

public class TestRingBuffer {

    @Test(timeout = 5000)
    public void testSimple() throws InterruptedException {
        RingBuffer<Integer> rb = new RingBuffer<>(10, Integer.class);
        Assert.assertTrue(rb.isEmpty());
        Assert.assertNull(rb.peek());
        for (int i = 0; i < 16; i++) {
            Assert.assertTrue(rb.put(i));
        }
        boolean putted = rb.put(Integer.MAX_VALUE, 1, TimeUnit.MILLISECONDS);
        Assert.assertFalse(putted);
        for (int i = 0; i < 16; i++ ) {
            Integer io = rb.take();
            Assert.assertEquals(i, io.intValue());
        }
        Assert.assertTrue(rb.isEmpty());
    }

    @Test//(timeout = 5000)
    public void concurrent() throws InterruptedException {
        AtomicReference<Throwable> failed1 = new AtomicReference<>(null);
        AtomicReference<Throwable> failed2 = new AtomicReference<>(null);
        AtomicReference<Throwable> failed3 = new AtomicReference<>(null);
        RingBuffer<Integer> rb = new RingBuffer<>(16, Integer.class);

        Thread t1 = ThreadBuilder.get().setName("PutterThread").setTask(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    Assert.assertTrue(rb.put(i));
                }
            } catch (Throwable e) {
                failed2.set(e);
            }
        }).build(true);
        Thread t2 = ThreadBuilder.get().setName("TakerThread").setTask(() -> {
            try {
                for (int i = 0; i < 1000; i++ ) {
                    Integer io = rb.take();
                    Assert.assertEquals(i, io.intValue());
                }
            } catch (Throwable e) {
                failed2.set(e);
            }
        }).build(true);
        Thread t3 = ThreadBuilder.get().setName("WatcherThread").setTask(() -> {
            try {
                for (int i = 0; i < 1000; i++ ) {
                    rb.peek();
                    Thread.yield();
                }
            } catch (Throwable e) {
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

}
