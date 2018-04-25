package loghub.processors;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
import loghub.Expression.ExpressionException;
import loghub.Processor;
import loghub.ProcessorException;
import loghub.configuration.Properties;

public class Log extends Processor {

    private String message;
    private Expression expression;
    private Logger customLogger;
    private String pipeName;
    private Level level;

    @Override
    public boolean configure(Properties properties) {
        customLogger = LogManager.getLogger("loghub.eventlogger." + pipeName);
        try {
            expression = new Expression(message, properties.groovyClassLoader, properties.formatters);
        } catch (ExpressionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof CompilationFailedException) {
                logger.error("invalid groovy expression: {}", e.getMessage());
                return false;
            } else {
                logger.error("Critical groovy error: {}", e.getCause().getMessage());
                logger.throwing(Level.DEBUG, e.getCause());
                return false;
            }
        }
        return super.configure(properties);
    }

    @Override
    public boolean process(Event event) throws ProcessorException {
        if (customLogger.isEnabled(level)) {
            Map<String, Object> esjson = new HashMap<>(event.size());
            esjson.putAll(event);
            esjson.put("@timestamp", event.getTimestamp());
            customLogger.log(level, expression.eval(event));
        }
        return true;
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
    public String getMessage() {
        return message;
    }

    /**
     * @param message the expression to set
     */
    public void setMessage(String message) {
        this.message = message;
    }

}
