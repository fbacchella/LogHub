package loghub;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
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
import org.codehaus.groovy.runtime.StringGroovyMethods;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovySystem;
import groovy.lang.MetaClassRegistry;
import groovy.lang.Script;
import groovy.runtime.metaclass.java.lang.NumberMetaClass;
import groovy.runtime.metaclass.loghub.NullOrNoneValueMetaClass;
import lombok.Getter;

/**
 * Evaluate groovy expressions.
 * <p>
 * It uses an internal compiled cache, for lazy compilation. But it still check expression during instantiation
 * @author Fabrice Bacchella
 *
 */
public class Expression {

    static {
        MetaClassRegistry registry = GroovySystem.getMetaClassRegistry();

        for (Class<?> c: new Class[] {NullOrMissingValue.NULL.getClass(), NullOrMissingValue.MISSING.getClass()}) {
            registry.setMetaClass(c, new NullOrNoneValueMetaClass(c));
        }

        for (Class<?> c: new Class[] {Integer.class, Byte.class, Double.class, Float.class, Long.class, Short.class, BigDecimal.class, BigInteger.class}) {
            registry.setMetaClass(c, new NumberMetaClass(c));
        }
    }

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
        private Expression ex;
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
            case "formatters": return ex.formatters;
            case "ex": return ex;
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

    public Object eval(Event event) throws ProcessorException {
        logger.trace("Evaluating script {} with formatters {}", expression, formatters);
        BindingMap bmap = bindings.get();
        bmap.event = event;
        bmap.ex = this;
        Optional<Script> optls = Optional.empty();
        try {
            // Lazy compilation, will only compile if expression is needed
            optls = Optional.of(compilationCache.get().computeIfAbsent(expression, this::compile));
            Script localscript = optls.get();
            localscript.setBinding(bmap.binding);
            return Optional.ofNullable(localscript.run())
                           .map(o -> { if (o == NullOrMissingValue.MISSING) throw IgnoredEventException.INSTANCE; else return o;})
                           .map(o -> { if (o == NullOrMissingValue.NULL) return null; else return o;})
                           .orElse(null);
        } catch (UnsupportedOperationException e) {
            throw event.buildException(String.format("script compilation failed '%s': %s", expression, Helpers.resolveThrowableException(e.getCause())), e);
        } catch (IgnoredEventException e) {
            throw e;
        } catch (Exception e) {
            throw event.buildException(String.format("failed expression '%s': %s", expression, Helpers.resolveThrowableException(e)), e);
        } finally {
            optls.ifPresent(b -> b.setBinding(EMPTYBIDDING));
            bmap.ex = null;
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

    public Object protect(String op, Object arg) {
        switch (op) {
        case "**":
        case "*":
        case "/":
        case "+":
        case "-":
        case "<<":
        case ">>":
        case ">>>":
        case "<":
        case "<=":
        case ">":
        case ">=":
        case "<=>":
        case "^":
        case "&":
        case "|":
            if (arg == NullOrMissingValue.MISSING) {
                throw IgnoredEventException.INSTANCE;
            } else if (arg == null){
                return NullOrMissingValue.NULL;
            } else {
                return arg;
            }
        case "&&":
        case "||":
        case "==":
        case "===":
        case "!=":
            if (arg == null) {
                return NullOrMissingValue.NULL;
            } else {
                return arg;
            }
        default: return arg;
        }
    }

    public Object stringMethod(String method, Object arg) {
        if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else {
            boolean nullarg = arg == null || arg ==  NullOrMissingValue.NULL;
            switch (method) {
            case "trim":
                return nullarg ? NullOrMissingValue.NULL : arg.toString().trim();
            case "capitalize":
                return nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.capitalize(arg.toString());
            case "uncapitalize":
                return nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.uncapitalize(arg.toString());
            case "isBlank":
                return nullarg ? true : StringGroovyMethods.isAllWhitespace(arg.toString());
            case "normalize":
                return nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.normalize(arg.toString());
            default:
                assert false: method;
                // Can’t be reached
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public Object nullfilter(Object arg, String op) {
        if (arg == null) {
            return NullOrMissingValue.NULL;
        } else {
            return arg;
        }
    }


    public Object compare(String operator, Object arg1, Object arg2) {
        if (arg1 instanceof NullOrMissingValue || arg2 instanceof NullOrMissingValue) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg1 instanceof Comparable && arg2 instanceof Comparable){
            int compare = compareObjects(arg1, arg2);
            switch (operator) {
            case "<":
                return compare < 0;
            case ">":
                return compare > 0;
            case ">=":
                return compare >= 0;
            case "<=":
                return compare <= 0;
            case "<=>":
                return compare;
            default:
                assert false;
                throw IgnoredEventException.INSTANCE;
            }
        } else {
            throw IgnoredEventException.INSTANCE;
        }
    }
    
    private int compareObjects(Object arg1, Object arg2) {
        if (arg1 instanceof Float && arg2 instanceof Float) {
            return Float.compare(((Float)arg1).floatValue(), ((Float)arg2).floatValue());
        } else if (arg1 instanceof Double && arg2 instanceof Double) {
            return Double.compare(((Double)arg1).doubleValue(), ((Double)arg2).doubleValue());
        } else if (arg1 instanceof Float && arg2 instanceof Double
                   || arg1 instanceof Double && arg2 instanceof Float) {
            return Double.compare(((Number)arg1).doubleValue(), ((Number)arg2).doubleValue());
        } else if (arg1 instanceof BigInteger && arg2 instanceof BigInteger) {
            return ((BigInteger)arg1).compareTo((BigInteger)arg2);
        } else if (arg1 instanceof BigDecimal && arg2 instanceof BigDecimal) {
            return ((BigDecimal)arg1).compareTo((BigDecimal)arg2);
        } else if (arg1 instanceof Number && arg2 instanceof Number) {
            return Long.compare(((Number)arg1).longValue(), ((Number)arg2).longValue());
        } else if (arg1 instanceof Comparable && arg1.getClass().isAssignableFrom(arg2.getClass())) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            int compare = ((Comparable)arg1).compareTo(arg2);
            return compare;
        } else {
            throw IgnoredEventException.INSTANCE;
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
