package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"field"})
public abstract class Encoder {

    private static final StackLocator stacklocator = StackLocator.getInstance();

    protected final Logger logger;

    protected String field;

    protected Encoder() {
        logger = LogManager.getLogger(stacklocator.getCallerClass(2));
    }

    public boolean configure(Properties properties, Sender sender) {
        return true;
    }

    public abstract byte[] encode(Event event);

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
