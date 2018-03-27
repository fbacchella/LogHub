package loghub.receivers;

import java.util.concurrent.BlockingQueue;

import loghub.Event;
import loghub.Pipeline;
import loghub.kafka.KafkaReceiver;

public class Kafka extends KafkaReceiver {

    public Kafka(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

}
