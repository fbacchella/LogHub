package loghub.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public abstract class BlockingQueueBenchmark extends AbstractQueueBenchmark {

    private BlockingQueue<CountDownLatch> arrayBlockingQueue = new ArrayBlockingQueue<>(2048);

    @Override
    CountDownLatch poll()
    {
        return arrayBlockingQueue.poll();
    }

    @Override
    boolean put(CountDownLatch simpleEvent) throws InterruptedException {
        return arrayBlockingQueue.offer(simpleEvent, 1, TimeUnit.SECONDS);
    }

}
