package loghub;

import java.util.concurrent.BlockingQueue;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"encoder"})
public abstract class Sender extends Thread {

    private final BlockingQueue<Event> inQueue;
    private Encoder encoder;

    public Sender(BlockingQueue<Event> inQueue) {
        setDaemon(true);
        setName("sender-" + getSenderName());
        this.inQueue = inQueue;
    }

    public boolean configure(Properties properties) {
        return true;
    }

    public abstract boolean send(Event e);
    public abstract String getSenderName();

    public void run() {
        while (true) {
            try {
                Event event = inQueue.take();
                if(send(event)) {
                    Stats.sent.incrementAndGet();
                } else {
                    Stats.dropped.incrementAndGet();
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    public Encoder getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder codec) {
        this.encoder = codec;
    }

}