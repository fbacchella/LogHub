package loghub.processors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Event;
import loghub.Expression;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class Log extends Processor {

    private Expression expression = null;
    private Logger customLogger;
    private String pipeName;
    private Level level;

    @Override
    public boolean configure(Properties properties) {
        customLogger = LogManager.getLogger("loghub.eventlogger." + pipeName);
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (customLogger == null) {
            throw event.buildException("Undefined logger");
        } else if (customLogger.isEnabled(level)) {
            customLogger.log(level, expression.eval(event));
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String getName() {
        return "log";
    }

    /**
     * @return the level
     */
    public String getLevel() {
        return level.toString();
    }

    /**
     * @param level the level to set
     */
    public void setLevel(String level) {
        this.level = Level.toLevel(level);
    }

    /**
     * @return the pipeName
     */
    public String getPipeName() {
        return pipeName;
    }

    /**
     * @param pipeName the pipeName to set
     */
    public void setPipeName(String pipeName) {
        this.pipeName = pipeName;
    }

    /**
     * @return the message
     */
    public Expression getMessage() {
        return expression;
    }

    public void setMessage(Expression expression) {
        this.expression = expression;
    }

}
