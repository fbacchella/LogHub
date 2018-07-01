package loghub.encoders;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import loghub.AbstractBuilder;
import loghub.Event;
import loghub.configuration.Properties;
import loghub.senders.Sender;
import lombok.Setter;

public abstract class Encoder {

    public abstract static class Builder<B extends Encoder> extends AbstractBuilder<B> {
        @Setter
        private String field = "message";
    };

    private static final StackLocator stacklocator = StackLocator.getInstance();

    protected final Logger logger;
    
    protected final String field;

    protected Encoder(Builder<?  extends Encoder> builder) {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
        this.field = builder.field;
    }

    public boolean configure(Properties properties, Sender sender) {
        return true;
    }

    public abstract byte[] encode(Event event);

}
