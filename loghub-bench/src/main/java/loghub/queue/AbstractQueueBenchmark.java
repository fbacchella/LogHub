package loghub.queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import loghub.ThreadBuilder;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
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

}
