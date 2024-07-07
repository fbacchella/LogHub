package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(1)
public class QueueBenchmark1P5C extends BlockingQueueBenchmark {

    @Override
    int countConsumer()
    {
        return 5;
    }

}
