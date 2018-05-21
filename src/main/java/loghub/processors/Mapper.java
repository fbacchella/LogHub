package loghub.processors;

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.codehaus.groovy.control.CompilationFailedException;

import loghub.Event;
import loghub.Expression;
import loghub.ProcessorException;
import loghub.Expression.ExpressionException;
import loghub.configuration.Properties;

public class Mapper extends Etl {

    private Map<Object, Object> map;
    private String expression;

    private Expression script;

    @Override
    public boolean configure(Properties properties) {
        try {
            script = new Expression(expression, properties.groovyClassLoader, properties.formatters);
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
        Object key = script.eval(event);
        if(key == null) {
            return false;
        }
        // Map only uses integer number as key, as parsing number only generate integer
        // So ensure the the key is an integer
        // Ignore float/double case, floating point key don't make sense
        if (key instanceof Number && ! (key instanceof Integer) && ! (key instanceof Double) && ! (key instanceof Float)) {
            key = Integer.valueOf(((Number) key).intValue());
        }
        if (! map.containsKey(key)) {
            return false;
        }
        Object value =  map.get(key);
        event.applyAtPath((i,j,k) -> i.put(j, k), lvalue, value, true);
        return true;
    }

    /**
     * @return the map
     */
    public Map<Object, Object> getMap() {
        return map;
    }

    /**
     * @param map the map to set
     */
    public void setMap(Map<Object, Object> map) {
        this.map = map;
    }

    public String getExpression() {
        return expression;
    }
    public void setExpression(String expression) {
        this.expression = expression;
    }

}
