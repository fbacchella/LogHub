package loghub.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public  abstract class RingBufferBenchmark extends AbstractQueueBenchmark {

    private static RingBuffer<CountDownLatch> arrayBlockingQueue = new RingBuffer<>(2048, CountDownLatch.class);

    @Override
    CountDownLatch poll()
    {
        return arrayBlockingQueue.poll();
    }

    @Override
    boolean put(CountDownLatch latch)
    {
        return arrayBlockingQueue.put(latch, 1, TimeUnit.SECONDS);
    }

    public static void main(final String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(RingBufferBenchmark.class.getName())
                .include(BlockingQueueBenchmark.class.getName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

}
