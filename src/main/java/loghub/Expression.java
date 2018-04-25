package loghub;

import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;

public class Expression {

    /**
     * Used to wrap some too generic or RuntimeException and catch it, to have a better management
     * of expressions errors.
     * @author Fabrice Bacchella
     *
     */
    public static class ExpressionException extends Exception {
        public ExpressionException(Throwable cause) {
            super(cause);
        }
    }

    private static final Logger logger = LogManager.getLogger();

    private static final Binding EMPTYBIDDING = new Binding();

    private final String expression;
    private Map<String, VarFormatter> formatters;
    private final ThreadLocal<Script> groovyScript;

    @SuppressWarnings("unchecked")
    public Expression(String expression, GroovyClassLoader loader, Map<String, VarFormatter> formatters) throws ExpressionException {
        logger.trace("adding expression {}", expression);
        this.expression = expression;
        Class<Script> groovyClass;
        try {
            groovyClass = loader.parseClass(expression);
        } catch (CompilationFailedException e) {
            throw new ExpressionException(e);
        }
        groovyScript = ThreadLocal.withInitial(() -> {
            try {
                return groovyClass.newInstance();
            } catch (IllegalAccessException | InstantiationException e) {
                throw new UnsupportedOperationException(e);
            }
        });
        // Not a real null test, if it fails it will throw an UnsupportedOperationException
        // Try to catch it early instead of for each event
        // Just for too smart compiler
        try {
            if (groovyScript.get() == null) {
                throw new ExpressionException(new NullPointerException());
            }
        } catch (UnsupportedOperationException e) {
            throw new ExpressionException(e);
        }
        this.formatters = formatters;
    }

    public synchronized Object eval(Event event) throws ProcessorException {
        logger.trace("Evaluating script {} with formatters {}", expression, formatters);
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        groovyBinding.setVariable("formatters", formatters);
        Script localscript;
        try {
            localscript = groovyScript.get();
        } catch (UnsupportedOperationException e) {
            throw event.buildException(String.format("script compilation failed '%s': %s", expression, e.getCause().getMessage()));
        }
        localscript.setBinding(groovyBinding);
        try {
            return localscript.run();
        } catch (Exception e) {
            throw event.buildException(String.format("failed expression '%s': %s", expression, e.getMessage()));
        } finally {
            localscript.setBinding(EMPTYBIDDING);
        }
    }

    /**
     * @return the expression
     */
    public String getExpression() {
        return expression;
    }

    public static void logError(ExpressionException e, String source, Logger logger) {
        Throwable cause = e.getCause();
        if (cause instanceof CompilationFailedException) {
            logger.error("Groovy compilation failed for expression {}: {}", source, e.getMessage());
        } else {
            logger.error("Critical groovy error for expression {}: {}", source, e.getMessage());
            logger.throwing(Level.DEBUG, e.getCause());
        }
    }

}
