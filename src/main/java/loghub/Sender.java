package loghub;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.configuration.Beans;
import loghub.configuration.Properties;
import loghub.senders.AsyncSender;

@Beans({"encoder"})
public abstract class Sender extends Thread {

    protected final Logger logger;

    private final BlockingQueue<Event> inQueue;
    private Encoder encoder;
    private boolean isAsync;

    public Sender(BlockingQueue<Event> inQueue) {
        setDaemon(true);
        setName("sender-" + getSenderName());
        this.inQueue = inQueue;
        logger = LogManager.getLogger(Helpers.getFistInitClass());
        isAsync = getClass().getAnnotation(AsyncSender.class) != null;
    }

    public boolean configure(Properties properties) {
        if (encoder != null) {
            return encoder.configure(properties, this);
        } else {
            return true;
        }
    }


    public void stopSending() {
        interrupt();
    }

    public abstract boolean send(Event e);
    public abstract String getSenderName();

    public void run() {
        while (! isInterrupted()) {
            Event event = null;
            try {
                event = inQueue.take();
                boolean status = send(event);
                if (! isAsync) {
                    processStatus(event, CompletableFuture.completedFuture(status));
                }
                event = null;
            } catch (InterruptedException e) {
                interrupt();
                break;
            } catch (Exception | StackOverflowError e) {
                CompletableFuture<Boolean> failed = new CompletableFuture<>();
                failed.completeExceptionally(e);
                processStatus(event, failed);
            }
        }
    }

    public void processStatus(Event event, Future<Boolean> result) {
        try {
            if (result.get()) {
                Stats.sent.incrementAndGet();
            } else {
                Stats.failed.incrementAndGet();
            }
        } catch (InterruptedException e) {
            interrupt();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (Helpers.isFatal(cause)) {
                throw (Error) cause;
            }
            Stats.newException(e);
            logger.error("Send failed: {}", Helpers.resolveThrowableException(cause));
            logger.catching(Level.DEBUG, cause);
        }
        event.end();
    }

    public Encoder getEncoder() {
        return encoder;
    }

    public void setEncoder(Encoder codec) {
        this.encoder = codec;
    }
}
