package loghub;

import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"encoder"})
public abstract class Sender extends Thread {

    protected final Logger logger;

    private final BlockingQueue<Event> inQueue;
    private Encoder encoder;

    public Sender(BlockingQueue<Event> inQueue) {
        setDaemon(true);
        setName("sender-" + getSenderName());
        this.inQueue = inQueue;
        logger = LogManager.getLogger(Helpers.getFistInitClass());
    }

    public boolean configure(Properties properties) {
        if (encoder != null) {
            return encoder.configure(properties, this);
        } else {
            return true;
        }
    }

    public abstract boolean send(Event e);
    public abstract String getSenderName();

    public void run() {
        while (true) {
            Event event = null;
            try {
                event = inQueue.take();
                if(send(event)) {
                    Stats.sent.incrementAndGet();
                } else {
                    Stats.dropped.incrementAndGet();
                }
                event.end();
                event = null;
            } catch (InterruptedException e) {
                interrupt();
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