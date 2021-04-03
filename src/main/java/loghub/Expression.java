package loghub;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilationUnit;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import lombok.Getter;

/**
 * Evaluate groovy expressions.
 * <p>
 * It uses an internal compiled cache, for lazy compilation. But it still check expression during instantiation
 * @author Fabrice Bacchella
 *
 */
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
    private static final Map<String, ThreadLocal<Script>> compilationCache = new ConcurrentHashMap<>();

    @Getter
    private final String expression;
    private Map<String, VarFormatter> formatters;
    private final GroovyClassLoader loader;

    public Expression(String expression, GroovyClassLoader loader, Map<String, VarFormatter> formatters) throws ExpressionException {
        logger.trace("adding expression {}", expression);
        try {
            // Check the expression, but using a CompilationUnit is much faster than generating the execution class
            CompilationUnit cu = new CompilationUnit(loader);
            cu.addSource("", expression);
            cu.compile();
        } catch (CompilationFailedException ex) {
            throw new ExpressionException(ex);
        }
        this.expression = expression;
        this.loader = loader;
        this.formatters = formatters;
    }

    public synchronized Object eval(Event event) throws ProcessorException {
        logger.trace("Evaluating script {} with formatters {}", expression, formatters);
        Binding groovyBinding = new Binding();
        groovyBinding.setVariable("event", event);
        groovyBinding.setVariable("formatters", formatters);
        Script localscript;
        try {
            // Lazy compilation, will only compile if expression is needed
            localscript = compilationCache.computeIfAbsent(expression, this::compile).get();
        } catch (UnsupportedOperationException e) {
            throw event.buildException(String.format("script compilation failed '%s': %s", expression, Helpers.resolveThrowableException(e.getCause())), e);
        }
        localscript.setBinding(groovyBinding);
        try {
            return localscript.run();
        } catch (IgnoredEventException e) {
            throw e;
        } catch (Exception e) {
            throw event.buildException(String.format("failed expression '%s': %s", expression, Helpers.resolveThrowableException(e)));
        } finally {
            localscript.setBinding(EMPTYBIDDING);
        }
    }

    @SuppressWarnings("unchecked")
    private ThreadLocal<Script> compile(String unused) {
        Class<Script> groovyClass;
        try {
            groovyClass = loader.parseClass(expression);
        } catch (CompilationFailedException e) {
            throw new UnsupportedOperationException(new ExpressionException(e));
        }
        return ThreadLocal.withInitial(() -> {
            try {
                return groovyClass.getConstructor().newInstance();
            } catch (IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                throw new UnsupportedOperationException(e);
            }
        });
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

    /**
     * Clear the compilation cache
     */
    public static void clearCache() {
        compilationCache.clear();
    }

}
