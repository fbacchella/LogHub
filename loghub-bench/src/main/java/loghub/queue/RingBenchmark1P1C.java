package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(1)
public class RingBenchmark1P1C extends RingBufferBenchmark {

    @Override
    int countConsumer()
    {
        return 1;
    }

}
