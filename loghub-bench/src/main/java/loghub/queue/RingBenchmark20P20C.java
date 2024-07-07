package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(20)
public class RingBenchmark20P20C extends RingBufferBenchmark {

    @Override
    int countConsumer()
    {
        return 20;
    }

}
