package loghub.encoders;

import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import loghub.AbstractBuilder;
import loghub.CanBatch;
import loghub.events.Event;
import loghub.configuration.Properties;
import loghub.senders.Sender;
import lombok.Setter;

public abstract class Encoder {

    public abstract static class Builder<B extends Encoder> extends AbstractBuilder<B> {
        @Setter
        private String field = "message";
    }

    private static final StackLocator stacklocator = StackLocator.getInstance();

    protected final Logger logger;

    protected final String field;

    protected Encoder(Builder<?  extends Encoder> builder) {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
        this.field = builder.field;
    }

    public boolean configure(Properties properties, Sender sender) {
        if (getClass().getAnnotation(CanBatch.class) == null && sender.isWithBatch()) {
            logger.error("This encoder don't handle batches");
            return false;
        } else {
            return true;
        }
    }

    /**
     * A encoder to be used within processor, so don't check on sender
     * @param properties
     * @return
     */
    public boolean configure(Properties properties) {
        return true;
    }

    public byte[] encode(Stream<Event> events) throws EncodeException {
        throw new UnsupportedOperationException("Can't batch events");
    }

    public abstract byte[] encode(Event event) throws EncodeException;

}
