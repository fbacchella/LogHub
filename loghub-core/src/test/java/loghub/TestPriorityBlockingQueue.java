package loghub;

import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import loghub.events.Event;
import loghub.events.EventsFactory;

public class TestPriorityBlockingQueue {

    private final EventsFactory factory = new EventsFactory();

    @Test(timeout = 2000)
    public void test1() throws InterruptedException {
        PriorityBlockingQueue queue = new PriorityBlockingQueue(10, 2);
        CountDownLatch latch = new CountDownLatch(1);
        Event ev = factory.newEvent();
        ev.put("#sync", true);
        ThreadBuilder.get().setTask(() -> {
            try {
                latch.await();
                queue.putBlocking(ev);
            } catch (InterruptedException e) {
                // empty
            }
        }).build(true);
        for (int i = 0; i < 10; i++) {
            queue.put(factory.newEvent());
            Thread.sleep(50);
            if (i == 5) {
                latch.countDown();
            }
        }
        long count = queue.stream().collect(Collectors.counting());
        Assert.assertEquals(11L, count);
        Event[] result = queue.toArray(new Event[0]);
        for (int i = 0; i < 11; i++) {
            if (result[i].containsKey("#sync")) {
                Assert.assertEquals(String.valueOf(i), 9, i);
                break;
            }
        }
    }

    @Test(timeout = 2000)
    public void test2() throws InterruptedException {
        PriorityBlockingQueue queue = new PriorityBlockingQueue(11, 0);
        CountDownLatch latch = new CountDownLatch(1);
        Event ev = factory.newEvent();
        ev.put("#sync", true);
        ThreadBuilder.get().setTask(() -> {
            try {
                latch.await();
                queue.putBlocking(ev);
            } catch (InterruptedException e) {
                // empty
            }
        }).build(true);
        for (int i = 0; i < 10; i++) {
            queue.put(factory.newEvent());
            Thread.sleep(50);
            if (i == 5) {
                latch.countDown();
            }
        }
        long count = queue.stream().collect(Collectors.counting());
        Assert.assertEquals(11L, count);
        Event[] result = queue.toArray(new Event[0]);
        for (int i = 0; i < 11; i++) {
            if (result[i].containsKey("#sync")) {
                Assert.assertTrue(String.valueOf(i), i >= 6 && i <= 7);
                break;
            }
        }
    }

}
