package loghub.queue;

import org.openjdk.jmh.annotations.Threads;

@Threads(5)
public class QueueBenchmark20P20C extends BlockingQueueBenchmark {

    @Override
    int countConsumer()
    {
        return 5;
    }

}
