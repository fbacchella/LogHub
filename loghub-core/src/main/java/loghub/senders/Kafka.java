package loghub.senders;

import java.util.concurrent.BlockingQueue;

import loghub.Event;
import loghub.kafka.KakfaSender;

public class Kafka extends KakfaSender {

    public Kafka(BlockingQueue<Event> inQueue) {
        super(inQueue);
    }

}
