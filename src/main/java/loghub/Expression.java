package loghub;

import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    private static class BindingMap extends AbstractMap<String, Object> {

        private Event event;
        private Map<String, VarFormatter> formatters;
        private final Binding binding;
        BindingMap() {
            this.binding = new Binding(this);
        }
        @Override
        public Set<Entry<String, Object>> entrySet() {
            return Collections.emptySet();
        }
        @Override
        public Object get(Object key) {
            switch (key.toString()) {
            case "event": return event;
            case "formatters": return formatters;
            default: return null;
            }
        }

    }

    private static final Logger logger = LogManager.getLogger();

    private static final Binding EMPTYBIDDING = new Binding();
    private static final Set<Map<String, Script>> scriptsMaps = new HashSet<>();
    private static final ThreadLocal<Map<String, Script>> compilationCache = ThreadLocal.withInitial(() -> {
        Map<String, Script> m = new HashMap<>();
        synchronized(scriptsMaps) {
            scriptsMaps.add(m);
        }
        return m;
    });
    private static final ThreadLocal<BindingMap> bindings = ThreadLocal.withInitial(BindingMap::new);

    @Getter
    private final String expression;
    private final Map<String, VarFormatter> formatters;
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
        BindingMap bmap = bindings.get();
        bmap.formatters = this.formatters;
        bmap.event = event;
        Optional<Script> optls = Optional.empty();
        try {
            // Lazy compilation, will only compile if expression is needed
            Script localscript = Optional.of(compilationCache.get().computeIfAbsent(expression, this::compile)).get();
            localscript.setBinding(bmap.binding);
            return localscript.run();
        } catch (UnsupportedOperationException e) {
            throw event.buildException(String.format("script compilation failed '%s': %s", expression, Helpers.resolveThrowableException(e.getCause())), e);
        } catch (IgnoredEventException e) {
            throw e;
        } catch (Exception e) {
            throw event.buildException(String.format("failed expression '%s': %s", expression, Helpers.resolveThrowableException(e)));
        } finally {
            optls.ifPresent(b -> b.setBinding(EMPTYBIDDING));
            bmap.formatters = null;
            bmap.event = null;
        }
    }

    @SuppressWarnings("unchecked")
    private Script compile(String unused) {
        try {
            Class<Script> groovyClass = loader.parseClass(expression);
            return groovyClass.getConstructor().newInstance();
        } catch (CompilationFailedException | IllegalAccessException | InstantiationException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new UnsupportedOperationException(new ExpressionException(e));
        }
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
        synchronized(scriptsMaps) {
            scriptsMaps.forEach(Map::clear);
        }
    }

}
