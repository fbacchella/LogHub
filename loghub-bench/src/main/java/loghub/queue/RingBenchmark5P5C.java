package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(5)
public class RingBenchmark5P5C extends RingBufferBenchmark {

    @Override
    int countConsumer()
    {
        return 5;
    }

}
