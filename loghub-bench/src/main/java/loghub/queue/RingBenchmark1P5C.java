package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(1)
public class RingBenchmark1P5C extends RingBufferBenchmark {

    @Override
    int countConsumer()
    {
        return 5;
    }

}
