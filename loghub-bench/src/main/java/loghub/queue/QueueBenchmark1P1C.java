package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(1)
public class QueueBenchmark1P1C extends BlockingQueueBenchmark {

    @Override
    int countConsumer()
    {
        return 1;
    }

}
