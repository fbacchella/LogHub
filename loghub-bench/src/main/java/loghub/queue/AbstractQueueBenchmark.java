package loghub.queue;

import java.util.concurrent.CountDownLatch;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import loghub.ThreadBuilder;

public abstract class AbstractQueueBenchmark {
    private volatile boolean consumerRunning;

    @Setup
    public void setup() throws InterruptedException
    {
        int count = countConsumer();
        CountDownLatch consumerStartedLatch = new CountDownLatch(count);
        consumerRunning = true;
        for (int i= 0; i < count ; i++) {
            ThreadBuilder.get()
                         .setDaemon(true)
                         .setName("Consumer" + i)
                         .setTask(() -> run(consumerStartedLatch))
                         .build(true);
        }
        consumerStartedLatch.await();
    }

    private void run(CountDownLatch consumerStartedLatch) {
        consumerStartedLatch.countDown();
        while (consumerRunning) {
            CountDownLatch latch = poll();
            if (latch != null) {
                latch.countDown();
            }
        }
    }

    abstract int countConsumer();

    abstract CountDownLatch poll();

    abstract boolean put(CountDownLatch simpleEvent) throws InterruptedException;

    @Benchmark
    public void producing() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        if (!put(latch)) {
            throw new IllegalStateException("Queue full, benchmark should not experience backpressure");
        }
        latch.await();
    }

    @TearDown
    public void tearDown()
    {
        consumerRunning = false;
    }

    public static void main(final String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include("loghub.queue.RingBenchmark.P.C")
                .include("loghub.queue.QueueBenchmark.P.C")
                .forks(2)
                //.measurementIterations(1)
                //.measurementTime(TimeValue.seconds(1))
                //.warmupTime(TimeValue.seconds(1))
                //.warmupIterations(1)
                .build();
        new Runner(opt).run();
    }

}
