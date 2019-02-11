package loghub.senders;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.AbstractBuilder;
import loghub.Event;
import loghub.Helpers;
import loghub.Stats;
import loghub.configuration.Properties;
import loghub.encoders.Encoder;
import lombok.Getter;
import lombok.Setter;

public abstract class Sender extends Thread implements Closeable {

    public abstract static class Builder<B extends Sender> extends AbstractBuilder<B> {
        @Setter
        private Encoder encoder;
    };

    protected final Logger logger;

    private BlockingQueue<Event> inQueue;
    @Getter
    private final Encoder encoder;
    private final boolean isAsync;

    public Sender(Builder<?  extends  Sender> builder) {
        setDaemon(true);
        setName("sender-" + getSenderName());
        logger = LogManager.getLogger(Helpers.getFirstInitClass());
        isAsync = getClass().getAnnotation(AsyncSender.class) != null;
        encoder = builder.encoder;
    }

    public boolean configure(Properties properties) {
        if (encoder != null) {
            return encoder.configure(properties, this);
        } else if (getClass().getAnnotation(SelfEncoder.class) == null) {
            logger.error("Missing encoder");
            return false;
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
    
    /**
     * A method that can be used inside custom {@link Sender#run()} for synchronous wait
     * 
     * @return a waiting event
     * @throws InterruptedException
     */
    protected Event getNext() throws InterruptedException {
        return inQueue.take();
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

    public void setInQueue(BlockingQueue<Event> inQueue) {
        this.inQueue = inQueue;
    }
    
    @Override
    public void close() {
        // Default to do nothing
    }

}
