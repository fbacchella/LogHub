package loghub;

import java.io.Closeable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.control.CompilationFailedException;
import org.codehaus.groovy.control.CompilationUnit;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation;
import org.codehaus.groovy.runtime.typehandling.NumberMath;

import groovy.lang.Binding;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovySystem;
import groovy.lang.MetaClass;
import groovy.lang.MetaClassRegistry;
import groovy.lang.MissingPropertyException;
import groovy.lang.Script;
import groovy.runtime.metaclass.GroovyOperators;
import groovy.runtime.metaclass.java.lang.BooleanMetaClass;
import groovy.runtime.metaclass.java.lang.CharacterMetaClass;
import groovy.runtime.metaclass.java.lang.NumberMetaClass;
import groovy.runtime.metaclass.java.lang.StringMetaClass;
import groovy.runtime.metaclass.java.net.InetAddressMetaClass;
import groovy.runtime.metaclass.java.util.CollectionMetaClass;
import groovy.runtime.metaclass.java.util.MapMetaClass;
import groovy.runtime.metaclass.loghub.EventMetaClass;
import groovy.runtime.metaclass.loghub.ExpressionMetaClass;
import groovy.runtime.metaclass.loghub.LambdaPropertyMetaClass;
import groovy.runtime.metaclass.loghub.NullOrNoneValueMetaClass;
import groovy.runtime.metaclass.loghub.TimeMetaClass;
import groovy.runtime.metaclass.loghub.VarFormatterMetaClass;
import loghub.events.Event;
import lombok.Getter;

/**
 * Evaluate groovy expressions.
 * <p>
 * It uses an internal compiled cache, for lazy compilation. But it still checks expression during instantiation
 * @author Fabrice Bacchella
 *
 */
public class Expression {

    private static final String FORMATTER = "__FORMATTER__";

    private static final MetaClassRegistry registry = GroovySystem.getMetaClassRegistry();

    static {
        java.util.Map<Class<?>, Function<MetaClass, MetaClass>> metaClassFactories = new HashMap<>();
        metaClassFactories.put(String.class, StringMetaClass::new);
        metaClassFactories.put(Character.class, CharacterMetaClass::new);
        metaClassFactories.put(Expression.class, ExpressionMetaClass::new);
        metaClassFactories.put(VarFormatter.class, VarFormatterMetaClass::new);
        metaClassFactories.put(Boolean.class, BooleanMetaClass::new);
        metaClassFactories.put(NullOrMissingValue.NULL.getClass(), NullOrNoneValueMetaClass::new);
        metaClassFactories.put(NullOrMissingValue.MISSING.getClass(), NullOrNoneValueMetaClass::new);
        metaClassFactories.put(InetAddress.class, InetAddressMetaClass::new);
        metaClassFactories.put(Date.class, TimeMetaClass::new);
        metaClassFactories.put(Temporal.class, TimeMetaClass::new);
        metaClassFactories.put(Number.class, NumberMetaClass::new);
        metaClassFactories.put(Map.class, MapMetaClass::new);
        metaClassFactories.put(Collection.class, CollectionMetaClass::new);
        metaClassFactories.put(NullOrMissingValue.class,
                mc -> new LambdaPropertyMetaClass(mc, java.util.Map.ofEntries(
                    java.util.Map.entry("NULL", o -> NullOrMissingValue.NULL),
                    java.util.Map.entry("MISSING", o -> NullOrMissingValue.MISSING))
                )
        );
        metaClassFactories.put(ConnectionContext.class,
                mc -> new LambdaPropertyMetaClass(mc, java.util.Map.ofEntries(
                        java.util.Map.entry("principal", o -> ((ConnectionContext<?>)o).getPrincipal()),
                        java.util.Map.entry("localAddress", o -> ((ConnectionContext<?>)o).getLocalAddress()),
                        java.util.Map.entry("remoteAddress", o -> ((ConnectionContext<?>)o).getRemoteAddress())
                    )
                )
        );
        metaClassFactories.put(Principal.class,
                mc -> new LambdaPropertyMetaClass(mc, java.util.Map.ofEntries(
                        java.util.Map.entry("name", o -> ((Principal)o).getName())
                    )
                )
        );
        metaClassFactories.put(InetAddress.class,
                mc -> new LambdaPropertyMetaClass(mc, java.util.Map.ofEntries(
                        java.util.Map.entry("hostAddress", o -> ((InetAddress)o).getHostAddress())
                    )
                )
        );
        metaClassFactories.put(InetSocketAddress.class,
                mc -> new LambdaPropertyMetaClass(mc, java.util.Map.ofEntries(
                        java.util.Map.entry("hostAddress", o -> ((InetSocketAddress)o).getAddress())
                    )
                )
        );
        metaClassFactories.put(Event.class, EventMetaClass::new);

        registry.setMetaClassCreationHandle(new MetaClassRegistry.MetaClassCreationHandle() {
            @Override
            protected MetaClass createNormalMetaClass(Class theClass, MetaClassRegistry registry) {
                if (metaClassFactories.containsKey(theClass)) {
                    return doCreate(theClass, theClass);
                } else if (Event.class.isAssignableFrom(theClass)) {
                    return doCreate(Event.class, theClass);
                } else if (Temporal.class.isAssignableFrom(theClass)) {
                    return doCreate(Temporal.class, theClass);
                } else if (Number.class.isAssignableFrom(theClass)) {
                    return doCreate(Number.class, theClass);
                } else if (Collection.class.isAssignableFrom(theClass)) {
                    return doCreate(Collection.class, theClass);
                } else if (ConnectionContext.class.isAssignableFrom(theClass)) {
                    return doCreate(ConnectionContext.class, theClass);
                } else if (Principal.class.isAssignableFrom(theClass)) {
                    return doCreate(Principal.class, theClass);
                } else if (InetAddress.class.isAssignableFrom(theClass)) {
                    return doCreate(InetAddress.class, theClass);
                } else if (InetSocketAddress.class.isAssignableFrom(theClass)) {
                    return doCreate(InetSocketAddress.class, theClass);
                } else if (Map.class.isAssignableFrom(theClass)) {
                    return doCreate(Map.class, theClass);
                } else if (Script.class.isAssignableFrom(theClass)) {
                    return super.createNormalMetaClass(theClass, registry);
                } else {
                    logger.debug("Creating unhandler MetaClass {}", theClass::getName);
                    return super.createNormalMetaClass(theClass, registry);
                }
            }
            MetaClass doCreate(Class<?> key, Class<?> c) {
                logger.trace("Handling class {} with {}", c::getName, key::getName);
                return metaClassFactories.get(key).apply(super.createNormalMetaClass(c, registry));
            }
        });
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

    public interface ExpressionData {
        Event getEvent();
        Expression getExpression();
        Object getValue();
        java.util.Map<String, VarFormatter> getFormatters();
    }

    private static class BindingMap extends AbstractMap<String, Object> implements ExpressionData, Closeable {
        @Getter
        private Event event;
        @Getter
        private Expression expression;
        @Getter
        private Object value;
        private Script script;
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
            case "formatters": return expression.formatters;
            case "ex": return expression;
            case "value": return value;
            default: throw new MissingPropertyException(key + expression.expression, null);
            }
        }

        @Override
        public java.util.Map<String, VarFormatter> getFormatters() {
            return expression.formatters;
        }

        @Override
        public void close() {
            expression = null;
            event = null;
            value = null;
            Optional.ofNullable(script).ifPresent(s -> s.setBinding(EMPTYBIDDING));
            script = null;
        }
    }

    public interface ExpressionLambda {
        Object apply(ExpressionData data);
    }

    private static final Logger logger = LogManager.getLogger();

    private static final Binding EMPTYBIDDING = new Binding();
    private static final Set<java.util.Map<String, Script>> scriptsMaps = new HashSet<>();
    private static final ThreadLocal<java.util.Map<String, Script>> compilationCache = ThreadLocal.withInitial(() -> {
        java.util.Map<String, Script> m = new HashMap<>();
        synchronized(scriptsMaps) {
            scriptsMaps.add(m);
        }
        return m;
    });
    private static final ThreadLocal<BindingMap> bindings = ThreadLocal.withInitial(BindingMap::new);
    private static final java.util.Map<String, Pattern> PATTERN_CACHE = new ConcurrentHashMap<>();
    private static final java.util.Map<String, ThreadLocal<Matcher>> MATCHER_CACHE = new ConcurrentHashMap<>();
    private static final Pattern VARPATH_PATTERN = Pattern.compile("event.getGroovyPath\\((\\d+)\\)");

    @Getter
    private final String expression;
    private final java.util.Map<String, VarFormatter> formatters;
    private final GroovyClassLoader loader;
    private final Object literal;

    public Expression(String expression, GroovyClassLoader loader, java.util.Map<String, VarFormatter> formatters) throws ExpressionException {
        Matcher m = VARPATH_PATTERN.matcher(expression);
        if (m.matches()) {
            int vpid = Integer.parseInt(m.group(1));
            literal = VariablePath.getById(vpid);
        } else {
            this.literal = null;
            logger.trace("adding expression {}", expression);
            try {
                // Check the expression, but using a CompilationUnit is much faster than generating the execution class
                CompilationUnit cu = new CompilationUnit(loader);
                cu.addSource("", expression);
                cu.compile();
            } catch (CompilationFailedException ex) {
                throw new ExpressionException(ex);
            }
        }
        this.expression = expression;
        this.loader = loader;
        this.formatters = formatters;
    }

    /**
     * The parser can send a literal when an expression was expected. Just store it to be returned.
     * If it's a String value, it's easier to handle it as a formatter that will be applied to the event.
     *
     * @param literal the literal value
     */
    public Expression(Object literal) {
        if (literal instanceof String) {
            VarFormatter vf = new VarFormatter((String) literal);
            if (! vf.isEmpty()) {
                this.literal = vf;
                this.expression = (String) literal;
            } else {
                this.literal = literal;
                this.expression = null;
            }
        } else {
            this.literal = literal == null ? NullOrMissingValue.NULL : literal;
            this.expression = null;
        }
        this.loader = null;
        this.formatters = java.util.Map.of();
    }

    public Expression(VarFormatter format, java.util.Map<String, VarFormatter> formatters) {
        this.literal = format;
        this.expression = null;
        this.loader = null;
        this.formatters = formatters;
    }


    public Object eval(Event event) throws ProcessorException {
        return eval(event, null);
    }

    public Object eval(Event event, Object value) throws ProcessorException {
        if (literal instanceof VariablePath) {
            return Optional.ofNullable(event.getAtPath((VariablePath)literal))
                           .map(o -> { if (o == NullOrMissingValue.MISSING) throw IgnoredEventException.INSTANCE; else return o;})
                           .map(o -> { if (o == NullOrMissingValue.NULL) return null; else return o;})
                           .orElse(null);
        } else if (literal instanceof VarFormatter) {
            return ((VarFormatter) literal).format(event);
        } else if (literal instanceof ExpressionLambda) {
            ExpressionLambda lambda = (ExpressionLambda) literal;
            try (BindingMap bmap = resolveBindings(event, value)) {
                Object o = lambda.apply(bmap);
                return o == NullOrMissingValue.NULL ? null : o;
            } catch (IgnoredEventException e) {
                throw e;
            } catch (RuntimeException ex) {
                throw event.buildException(String.format("failed expression '%s': %s", expression, Helpers.resolveThrowableException(ex)), ex);
            }
        } else if (literal == NullOrMissingValue.NULL) {
            return null;
        } else if (literal != null) {
            // It's a constant expression, no need to evaluate it
            return literal;
        } else if (formatters.containsKey(FORMATTER)) {
            // A string expression was given, it might have been a format string to apply to the event
            return this.formatters.get(FORMATTER).format(event);
        } else {
            logger.trace("Evaluating script {} with formatters {}", expression, formatters);
            try (BindingMap bmap = resolveBindings(event, value)) {
                // Lazy compilation, will only compile if expression is needed
                return Optional.ofNullable(bmap.script.run())
                        .map(o -> { if (o == NullOrMissingValue.MISSING) throw IgnoredEventException.INSTANCE; else return o;})
                        .map(o -> { if (o == NullOrMissingValue.NULL) return null; else return o;})
                        .orElse(null);
            } catch (UnsupportedOperationException e) {
                throw event.buildException(String.format("script compilation failed '%s': %s", expression, Helpers.resolveThrowableException(e.getCause())), e);
            } catch (IgnoredEventException e) {
                throw e;
            } catch (Exception e) {
                throw event.buildException(String.format("failed expression '%s': %s", expression, Helpers.resolveThrowableException(e)), e);
            }
        }
    }

    private BindingMap resolveBindings(Event event, Object value) {
        BindingMap bmap = bindings.get();
        bmap.event = event;
        bmap.value = value;
        bmap.expression = this;
        if (expression != null) {
            bmap.script = compilationCache.get().computeIfAbsent(expression, this::compile);
            bmap.script.setBinding(bmap.binding);
        } else {
            bmap.script = null;
        }
        return bmap;
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
            } else
                return Objects.requireNonNullElse(arg, NullOrMissingValue.NULL);
        case "&&":
        case "||":
        case "==":
        case "===":
        case "!=":
            return Objects.requireNonNullElse(arg, NullOrMissingValue.NULL);
        default: return arg;
        }
    }

    public Object stringFunction(String method, Object arg) {
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
                return nullarg || StringGroovyMethods.isAllWhitespace(arg.toString());
            case "normalize":
                return nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.normalize(arg.toString());
            case "lowercase":
                return nullarg ? NullOrMissingValue.NULL : arg.toString().toLowerCase();
            case "uppercase":
                return nullarg ? NullOrMissingValue.NULL : arg.toString().toUpperCase();
            default:
                assert false: method;
                // Can’t be reached
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public Object join(Object arg1, Object arg2) {
        if (arg1 == NullOrMissingValue.MISSING || arg2 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg2 == null || arg2 == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else {
            Stream<String> strSrc = null;
            if (arg2 instanceof Collection) {
                Collection<?> list = (Collection<?>) arg2;
                strSrc = list.stream().map(Object::toString);
            } else if (arg2.getClass().isArray()) {
                strSrc = Arrays.stream(DefaultTypeTransformation.primitiveArrayBox(arg2)).map(Object::toString);
            }
            if (strSrc != null) {
                String separator = (arg1 != null && arg1 != NullOrMissingValue.NULL) ? arg1.toString(): "";
                return strSrc.collect(Collectors.joining(separator));
            } else {
                return arg2.toString();
            }
        }
    }

    public Object split(Object arg1, Object arg2) {
        if (arg1 == NullOrMissingValue.MISSING || arg2 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg1 == NullOrMissingValue.NULL || arg1 == null ) {
            return arg2;
        } else if (arg2 == null || arg2 == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else {
            return PATTERN_CACHE.computeIfAbsent(arg1.toString(), Pattern::compile).split(arg2.toString());
        }
    }

    public Object nullfilter(Object arg) {
        return Objects.requireNonNullElse(arg, NullOrMissingValue.NULL);
    }

    public Object instanceOf(String cmd, Object obj, Class<?> clazz) {
        boolean result;
        if (obj instanceof NullOrMissingValue || obj == null ) {
            result = false;
        } else {
            result = clazz.isAssignableFrom(obj.getClass());
        }
        return cmd.startsWith("!") != result;
    }

    public Object in(String cmd, Object obj1, Object obj2) {
        boolean result;
        if ((obj1 == null || obj1 == NullOrMissingValue.NULL) && (obj2 == null || obj2 == NullOrMissingValue.NULL)) {
            result = !cmd.startsWith("!");
        } else if (obj1 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (obj2 instanceof Collection) {
            result = ((Collection<?>)obj2).contains(obj1);
        } else if (obj2.getClass().isArray()) {
            result = DefaultTypeTransformation.primitiveArrayToList(obj2).contains(obj1);
        } else if ((obj1 instanceof CharSequence || obj1 instanceof Character) && obj2 instanceof CharSequence) {
            result = obj2.toString().contains(obj1.toString());
        } else {
            result = false;
        }
        return cmd.startsWith("!") != result;
    }

    public Object newCollection(String collectionType) {
        if ("set".equals(collectionType)) {
            return new LinkedHashSet<>();
        } else if ("list".equals(collectionType)) {
            return new ArrayList<>();
        } else {
            assert true: "unreachable";
            throw IgnoredEventException.INSTANCE;
        }
    }

    @SuppressWarnings("unchecked")
    public <T> Object asCollection(String collectionType, Object argument) {
        if ("set".equals(collectionType)) {
            if (argument instanceof Set) {
                return argument;
            } else if (argument instanceof Collection) {
                return new LinkedHashSet<>((Collection<T>) argument);
            } else if (argument.getClass().isArray()) {
                return new LinkedHashSet<T>(DefaultTypeTransformation.primitiveArrayToList(argument));
            } else {
                return new LinkedHashSet<>(Set.of((T)argument));
            }
        } else if ("list".equals(collectionType)) {
            if (argument instanceof List) {
                return argument;
            } else if (argument instanceof Collection) {
                return new ArrayList<>((Collection<T>) argument);
            } else if (argument.getClass().isArray()) {
                return new ArrayList<T>(DefaultTypeTransformation.primitiveArrayToList(argument));
            } else {
                return new ArrayList<>(List.of((T) argument));
            }
        } else {
            assert true: "unreachable";
            throw IgnoredEventException.INSTANCE;
        }
    }

    public Object getIterableIndex(Object iterable, int index) {
        if (iterable == null || iterable == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else if (Object[].class.isAssignableFrom(iterable.getClass())) {
            Object[] a = (Object[]) iterable;
            int pos = index >= 0 ? index : (a.length + index);
            if (a.length > pos) {
                return a[pos];
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (iterable instanceof List) {
            List<?> l = (List<?>) iterable;
            int pos = index >= 0 ? index : (l.size() + index);
            if (l.size() > pos) {
                return l.get(pos);
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (iterable == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else {
            throw new IllegalArgumentException("Array operation on not iterable object");
        }
    }

    public boolean isEmpty(Object arg) {
        if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        }
        if (arg == null || arg == NullOrMissingValue.NULL) {
            return true;
        } else if (arg instanceof String) {
            return ((String) arg).isEmpty();
        } else if (arg instanceof Collection) {
            return ((Collection<?>) arg).isEmpty();
        } else if (arg instanceof java.util.Map) {
            return ((java.util.Map<?, ?>) arg).isEmpty();
        } else if (arg.getClass().isArray()) {
            return Array.getLength(arg) == 0;
        } else {
            return false;
        }
    }

    private Object checkStringIp(Object arg1, Object arg2) {
        try {
            if (arg1 instanceof InetAddress && arg2 instanceof String) {
                if (((String) arg2).startsWith("/")) {
                    arg2 = ((String) arg2).substring(1);
                }
                return InetAddress.getByName((String)arg2);
            } else {
                return arg2;
            }
        } catch (UnknownHostException e) {
            return arg2;
        }
    }

    public Object compare(String operator, Object arg1, Object arg2) {
        arg1 = nullfilter(arg1);
        arg2 = protect(operator, arg2);
        // Detect if comparing an IP with a String, try to compare both as InetAddress
        arg2 = checkStringIp(arg1, arg2);
        arg1 = checkStringIp(arg2, arg1);
        boolean dateCompare = (arg1 instanceof Date || arg1 instanceof TemporalAccessor) &&
                              (arg2 instanceof Date || arg2 instanceof TemporalAccessor);
        if (arg1 == NullOrMissingValue.MISSING || arg2 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if ("==".equals(operator) && !dateCompare) {
            return arg1.equals(arg2);
        } else if ("!=".equals(operator) && !dateCompare) {
            return ! arg1.equals(arg2);
        } else if (dateCompare || (arg1 instanceof Comparable && arg2 instanceof Comparable)){
            int compare = compareObjects(arg1, arg2);
            switch (operator) {
            case "==":
                return compare == 0;
            case "!=":
                return compare != 0;
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
                assert false : String.format("%s %s %s", arg1, operator, arg2);
                throw IgnoredEventException.INSTANCE;
            }
        } else {
            throw IgnoredEventException.INSTANCE;
        }
    }

    private int compareObjects(Object arg1, Object arg2) {
        if (arg1 instanceof Number && arg2 instanceof Number) {
            return NumberMath.compareTo((Number)arg1, (Number)arg2);
        } else if (arg1 instanceof Date && arg2 instanceof TemporalAccessor) {
            try {
                long t1 = ((Date)arg1).getTime();
                long t2 = Instant.from((TemporalAccessor)arg2).toEpochMilli();
                return Long.compare(t1, t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg2 instanceof Date && arg1 instanceof TemporalAccessor) {
            try {
                long t2 = ((Date)arg2).getTime();
                long t1 = Instant.from((TemporalAccessor)arg1).toEpochMilli();
                return Long.compare(t1, t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg1 instanceof TemporalAccessor && arg2 instanceof TemporalAccessor) {
            // Groovy can't compare Instant and ZonedDateTime
            try {
                Instant t1 = Instant.from((TemporalAccessor)arg1);
                Instant t2 = Instant.from((TemporalAccessor)arg2);
                return t1.compareTo(t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg1 instanceof Comparable && arg1.getClass().isAssignableFrom(arg2.getClass())) {
            try {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                int compare = ((Comparable)arg1).compareTo(arg2);
                return compare;
            } catch (ClassCastException ex1) {
                try {
                    // Perhaps groovy is smarter
                    return DefaultTypeTransformation.compareTo(arg1, arg2);
                } catch (Exception ex2) {
                    throw IgnoredEventException.INSTANCE;
                }
            }
        } else {
            try {
                //Used as the last hope, it's very slow
                return DefaultTypeTransformation.compareTo(arg1, arg2);
            } catch (Exception e) {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public Object regex(Object arg, String op, String encodedPattern) {
        if (arg == NullOrMissingValue.NULL || arg == null || arg instanceof Collection || arg instanceof java.util.Map || arg.getClass().isArray()) {
            return false;
        } else if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else {
            Matcher m = MATCHER_CACHE.computeIfAbsent(encodedPattern, k -> {
                byte[] patternBytes = Base64.getDecoder().decode(k);
                String pattern = new String(patternBytes, StandardCharsets.UTF_8);
                return ThreadLocal.withInitial(() -> PATTERN_CACHE.computeIfAbsent(pattern, Pattern::compile).matcher(""));
            }).get();
            m.reset(arg.toString());
            if ("==~".equals(op)) {
                return m.matches();
            } else if ("=~".equals(op) && m.find()) {
                String[] groups = new String[m.groupCount() + 1];
                for (int i = 0; i < groups.length ; i++) {
                    groups[i] = m.group(i);
                }
                return groups;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public boolean asBoolean(Object arg) {
        if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg instanceof Boolean) {
            return (Boolean) arg;
        } else if (arg instanceof Float || arg instanceof Double || arg instanceof BigDecimal) {
            return ((Number) arg).doubleValue() != 0;
        } else if (arg instanceof Number) {
            return ((Number) arg).longValue() != 0;
        } else {
            return ! isEmpty(arg);
        }
    }

    public Object groovyOperator(String operator, Object arg1) {
        arg1 = nullfilter(arg1);
        if (arg1 == NullOrMissingValue.NULL) {
            throw IgnoredEventException.INSTANCE;
        }
        MetaClass mc = registry.getMetaClass(arg1.getClass());
        String groovyName;
        switch (operator) {
        case "~":
            groovyName = GroovyOperators.BITWISE_NEGATE;
            break;
        default:
            throw new UnsupportedOperationException(operator);
        }
        return mc.invokeMethod(arg1, groovyName, new Object[]{});
    }

    public Object groovyOperator(String operator, Object arg1, Object arg2) {
        arg1 = nullfilter(arg1);
        arg2 = protect(operator, arg2);
        if ("===".equals(operator) || "!==".equals(operator)) {
            return (System.identityHashCode(arg1) == System.identityHashCode(arg2)) ^ ("!==".equals(operator));
        } else {
            if (arg1 == NullOrMissingValue.NULL) {
                throw IgnoredEventException.INSTANCE;
            }
            MetaClass mc = registry.getMetaClass(arg1.getClass());
            String groovyName;
            switch (operator) {
            case "+":
                groovyName = GroovyOperators.PLUS;
                break;
            case "*":
                groovyName = GroovyOperators.MULTIPLY;
                break;
            case "/":
                groovyName = GroovyOperators.DIV;
                break;
            case "-":
                groovyName = GroovyOperators.MINUS;
                break;
            case "%":
                groovyName = GroovyOperators.MOD;
                break;
            case "**":
                groovyName = GroovyOperators.POWER;
                break;
            case "<<":
                groovyName = GroovyOperators.LEFT_SHIFT;
                break;
            case ">>":
                groovyName = GroovyOperators.RIGHT_SHIFT;
                break;
            case ">>>":
                groovyName = GroovyOperators.RIGHT_SHIFT_UNSIGNED;
                break;
            case "^":
                groovyName = GroovyOperators.XOR;
                break;
            case "|":
                groovyName = GroovyOperators.OR;
                break;
            case "&":
                groovyName = GroovyOperators.AND;
                break;
            default:
                throw new UnsupportedOperationException(operator);
            }
            return mc.invokeMethod(arg1, groovyName, new Object[]{arg2});
        }
    }

    public static void logError(ExpressionException e, String source, Logger logger) {
        Throwable cause = e.getCause();
        if (cause instanceof CompilationFailedException) {
            logger.error("Groovy compilation failed for expression {}: {}", () -> source, e::getMessage);
        } else {
            logger.error("Critical groovy error for expression {}: {}", () -> source, () -> Helpers.resolveThrowableException(cause));
            logger.throwing(Level.DEBUG, e.getCause());
        }
    }

    /**
     * Clear the compilation cache
     */
    public static void clearCache() {
        synchronized(scriptsMaps) {
            scriptsMaps.forEach(java.util.Map::clear);
        }
        MATCHER_CACHE.clear();
        PATTERN_CACHE.clear();
    }

}
