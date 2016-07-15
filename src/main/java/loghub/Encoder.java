package loghub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.ReflectionUtil;

import loghub.configuration.Beans;
import loghub.configuration.Properties;

@Beans({"field"})
public abstract class Encoder {

    protected final Logger logger;

    protected String field;
    
    protected Encoder() {
        logger = LogManager.getLogger(ReflectionUtil.getCallerClass(2));
    }

    public boolean configure(Properties properties) {
        return true;
    }

    abstract public byte[] encode(Event event);

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

}
