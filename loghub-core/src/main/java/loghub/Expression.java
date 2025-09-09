package loghub;

import java.io.Closeable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.groovy.runtime.StringGroovyMethods;
import org.codehaus.groovy.runtime.typehandling.DefaultTypeTransformation;
import org.codehaus.groovy.runtime.typehandling.NumberMath;

import groovy.lang.GroovySystem;
import groovy.lang.MetaClass;
import groovy.lang.MetaClassRegistry;
import io.netty.util.NetUtil;
import loghub.configuration.BeansManager;
import loghub.events.Event;
import loghub.groovy.BooleanMetaClass;
import loghub.groovy.CharacterMetaClass;
import loghub.groovy.CollectionMetaClass;
import loghub.groovy.GroovyMethods;
import loghub.groovy.LoghubMetaClass;
import loghub.groovy.NumberMetaClass;
import loghub.groovy.ObjectMetaClass;
import loghub.groovy.StringMetaClass;
import loghub.groovy.TemporalMetaClass;
import loghub.types.MacAddress;
import lombok.Getter;

/**
 * Evaluate groovy expressions.
 * <p>
 * It uses an internal compiled cache, for lazy compilation. But it still checks expression during instantiation
 * @author Fabrice Bacchella
 *
 */
public class Expression {

    private static final MetaClassRegistry registry = GroovySystem.getMetaClassRegistry();

    private static final Logger logger = LogManager.getLogger();

    static {
        java.util.Map<Class<?>, Function<MetaClass, MetaClass>> metaClassFactories = new HashMap<>();
        metaClassFactories.put(CharSequence.class, StringMetaClass::new);
        metaClassFactories.put(Character.class, CharacterMetaClass::new);
        metaClassFactories.put(Date.class, TemporalMetaClass::new);
        metaClassFactories.put(Temporal.class, TemporalMetaClass::new);
        metaClassFactories.put(Number.class, NumberMetaClass::new);
        metaClassFactories.put(Boolean.class, BooleanMetaClass::new);
        metaClassFactories.put(Collection.class, CollectionMetaClass::new);
        metaClassFactories.put(Object.class, ObjectMetaClass::new);

        registry.setMetaClassCreationHandle(new MetaClassRegistry.MetaClassCreationHandle() {
            @Override
            protected MetaClass createNormalMetaClass(Class theClass, MetaClassRegistry registry) {
                if (metaClassFactories.containsKey(theClass)) {
                    return doCreate(theClass, theClass);
                } else if (Temporal.class.isAssignableFrom(theClass)) {
                    return doCreate(Temporal.class, theClass);
                } else if (Number.class.isAssignableFrom(theClass)) {
                    return doCreate(Number.class, theClass);
                } else if (Collection.class.isAssignableFrom(theClass)) {
                    return doCreate(Collection.class, theClass);
                } else if (CharSequence.class.isAssignableFrom(theClass)) {
                    return doCreate(CharSequence.class, theClass);
                } else if (theClass.isArray()) {
                    return doCreate(Collection.class, theClass);
                } else {
                    logger.debug("Creating unhandled MetaClass {}", theClass::getName);
                    return doCreate(Object.class, theClass);
                }
            }
            MetaClass doCreate(Class<?> key, Class<?> c) {
                logger.trace("Handling class {} with {}", c::getName, key::getName);
                return metaClassFactories.get(key).apply(super.createNormalMetaClass(c, registry));
            }
        });
        registry.getMetaClassCreationHandler().setDisableCustomMetaClassLookup(true);
    }

    public interface ExpressionData {
        Event getEvent();
        Expression getExpression();
        Object getValue();
    }

    public static final ExpressionData EMPTY_EXPRESSION_DATA = new Expression.ExpressionData() {
        @Override
        public Event getEvent() {
            return null;
        }
        @Override
        public Expression getExpression() {
            return null;
        }
        @Override
        public Object getValue() {
            return null;
        }
    };

    @Getter
    private static class BindingMap implements ExpressionData, Closeable {
        private Event event;
        private Expression expression;
        private Object value;

        @Override
        public void close() {
            expression = null;
            event = null;
            value = null;
        }
    }

    public interface ExpressionLambda {
        Object apply(ExpressionData data);
    }

    public static final Object ANYVALUE = new Object();

    private static final ThreadLocal<BindingMap> bindings = ThreadLocal.withInitial(BindingMap::new);

    private static final Function<Pattern, Matcher> MATCHER_CACHE;
    private static final Runnable MATCHER_CLEAN;

    static {
        Set<Map<Pattern, Matcher>> maps = ConcurrentHashMap.newKeySet();
        ThreadLocal<Map<Pattern, Matcher>> cache = ThreadLocal.withInitial(() -> {
            Map<Pattern, Matcher> m = new HashMap<>();
            maps.add(m);
            return m;
        });
        MATCHER_CACHE = p -> cache.get().computeIfAbsent(p, k -> k.matcher(""));
        MATCHER_CLEAN = () -> maps.forEach(Map::clear);
    }

    private final ExpressionLambda evaluator;
    @Getter
    private final String source;

    public Expression(Object literal) {
        if (literal instanceof String) {
            this.source = String.format("\"%s\"", literal);
        } else if (literal instanceof Character) {
            this.source = String.format("'%s'", literal);
        } else if (literal == null || literal == NullOrMissingValue.NULL) {
            this.source = "null";
        } else {
            this.source = literal.toString();
        }
        this.evaluator = ed -> literal;
    }

    public Expression(String source, Object literal) {
        this.evaluator = ed -> literal;
        this.source = source;
    }

    public Expression(String source, VarFormatter format) {
        this.evaluator = ed -> format.format(ed.getEvent());
        this.source = source;
    }

    public Expression(VarFormatter format) {
        this.evaluator = ed -> format.format(ed.getEvent());
        this.source = format.toString();
    }

    public Expression(String source, VariablePath path) {
        this.evaluator = ed -> ed.getEvent().getAtPath(path);
        this.source = source;
    }

    public Expression(VariablePath path) {
        this.evaluator = ed -> ed.getEvent().getAtPath(path);
        this.source = path.toString();
    }

    public Expression(String source, ExpressionLambda evaluator) {
        this.evaluator = evaluator;
        this.source = source;
    }

    public Object eval() throws ProcessorException {
        return eval(null, null);
    }

    public Object eval(Event event) throws ProcessorException {
        return eval(event, null);
    }

    public Object eval(Event event, Object value) throws ProcessorException {
        try (BindingMap bmap = resolveBindings(event, value)) {
            return Optional.ofNullable(evaluator.apply(bmap))
                           .map(o -> { if (o == NullOrMissingValue.MISSING) throw IgnoredEventException.INSTANCE; else return o;})
                           .filter(o -> o != NullOrMissingValue.NULL)
                           .orElse(null);
        } catch (IgnoredEventException e) {
            throw e;
        } catch (RuntimeException ex) {
            throw event.buildException(String.format("Failed expression %s: %s", source, Helpers.resolveThrowableException(ex)), ex);
        }
    }

    private BindingMap resolveBindings(Event event, Object value) {
        BindingMap bmap = bindings.get();
        bmap.event = event;
        bmap.value = value;
        bmap.expression = this;
        return bmap;
    }

    public static Object protect(String op, Object arg) {
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
        case "!=":
        case "===":
        case "!==":
            return Objects.requireNonNullElse(arg, NullOrMissingValue.NULL);
        default: return arg;
        }
    }

    public static Object stringFunction(String method, Object arg) {
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
                // Can’t be reached
                assert false : method;
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public static Object gsub(Object apply, Pattern pattern, String replacement) {
        if (apply == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (apply == null || apply == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else {
            Matcher m = MATCHER_CACHE.apply(pattern);
            m.reset(apply.toString());
            return m.replaceAll(replacement);
        }
    }

    public static Object join(String separator, Object arg2) {
        if (arg2 == null || arg2 == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else if (arg2 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else {
            Stream<String> strSrc = null;
            if (arg2 instanceof Collection) {
                Collection<?> list = (Collection<?>) arg2;
                strSrc = list.stream().map(Object::toString);
            } else if (arg2.getClass().isArray()) {
                strSrc = Arrays.stream(DefaultTypeTransformation.primitiveArrayBox(arg2)).map(Object::toString);
            }
            if (strSrc != null) {
                return strSrc.collect(Collectors.joining(separator));
            } else {
                return arg2.toString();
            }
        }
    }

    public static Object split(Object arg1, Pattern pattern) {
        if (arg1 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg1 == NullOrMissingValue.NULL || arg1 == null) {
            return NullOrMissingValue.NULL;
        } else {
            return pattern.splitAsStream(arg1.toString()).collect(Collectors.toList());
        }
    }

    public static Object nullfilter(Object arg) {
        return Objects.requireNonNullElse(arg, NullOrMissingValue.NULL);
    }

    public static boolean instanceOf(boolean negated, Object obj, Class<?> clazz) {
        boolean result;
        if (obj instanceof NullOrMissingValue || obj == null) {
            result = false;
        } else {
            result = clazz.isAssignableFrom(obj.getClass());
        }
        return negated != result;
    }

    public static boolean in(String cmd, Object obj1, Object obj2) {
        boolean result;
        if (obj1 == NullOrMissingValue.MISSING || obj2 == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if ((obj1 == null || obj1 == NullOrMissingValue.NULL) && (obj2 == null || obj2 == NullOrMissingValue.NULL)) {
            result = !cmd.startsWith("!");
        } else if (obj2 instanceof Collection) {
            result = ((Collection<?>) obj2).contains(obj1);
        } else if (obj2.getClass().isArray()) {
            result = DefaultTypeTransformation.primitiveArrayToList(obj2).contains(obj1);
        } else if ((obj1 instanceof CharSequence || obj1 instanceof Character) && obj2 instanceof CharSequence) {
            result = obj2.toString().contains(obj1.toString());
        } else {
            result = false;
        }
        return cmd.startsWith("!") != result;
    }

    public static Object newCollection(String collectionType) {
        if ("set".equals(collectionType)) {
            return new LinkedHashSet<>();
        } else if ("list".equals(collectionType)) {
            return new ArrayList<>();
        } else {
            // Can’t be reached
            assert false : collectionType;
            throw IgnoredEventException.INSTANCE;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Object asCollection(String collectionType, Object argument) {
        if ("set".equals(collectionType)) {
            if (argument instanceof Set) {
                return argument;
            } else if (argument instanceof Collection) {
                return new LinkedHashSet<>((Collection<T>) argument);
            } else if (argument.getClass().isArray()) {
                return new LinkedHashSet<T>(DefaultTypeTransformation.primitiveArrayToList(argument));
            } else {
                return new LinkedHashSet<>(Set.of((T) argument));
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
            // Can’t be reached
            assert false : collectionType;
            throw IgnoredEventException.INSTANCE;
        }
    }

    public static Object getIterableIndex(Object iterable, int index) {
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

    public static boolean isEmpty(Object arg) {
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

    public static boolean isIpAddress(Object arg) {
        if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg == null || arg == NullOrMissingValue.NULL) {
            return false;
        } else if (arg instanceof String) {
            return NetUtil.isValidIpV4Address((String) arg) || NetUtil.isValidIpV6Address((String) arg);
        } else
            return arg instanceof InetAddress;
    }

    private static Object checkStringIp(Object arg1, Object arg2) {
        try {
            if ((arg1 instanceof InetAddress || arg1 instanceof InetAddress[]) && arg2 instanceof String) {
                if (((String) arg2).startsWith("/")) {
                    arg2 = ((String) arg2).substring(1);
                }
                return InetAddress.getAllByName((String) arg2);
            } else {
                return arg2;
            }
        } catch (UnknownHostException e) {
            return arg2;
        }
    }

    private enum ComparaisonClass {
        NULL,
        STRING,
        DATE,
        IP_ADDRESS,
        NUMBER,
        COLLECTION,
        COMPARABLE,
        OTHER;
        private static ComparaisonClass resolve(Object o) {
            if (o == null || o == NullOrMissingValue.NULL) {
                return NULL;
            } else if (o instanceof CharSequence) {
                return STRING;
            } else if (o instanceof Date || o instanceof TemporalAccessor) {
                return DATE;
            } else if (o instanceof InetAddress || o instanceof InetAddress[]) {
                return IP_ADDRESS;
            } else if (o instanceof Collection || o.getClass().isArray()) {
                return COLLECTION;
            } else if (o instanceof Number) {
                return NUMBER;
            } else if (o instanceof Comparable) {
                return COMPARABLE;
            } else {
                return OTHER;
            }
        }
    }

    public static Object compare(String operator, Object arg1, Object arg2) {
        if (arg2 == ANYVALUE) {
            return ((arg1 != NullOrMissingValue.MISSING) ^ "!=".equals(operator));
        } else if ((arg1 == NullOrMissingValue.MISSING || arg2 == NullOrMissingValue.MISSING) &&
                           ("==".equals(operator) || "===".equals(operator))) {
            return false;
        } else if ((arg1 == NullOrMissingValue.MISSING || arg2 == NullOrMissingValue.MISSING)) {
            throw IgnoredEventException.INSTANCE;
        } else if ("!==".equals(operator) || "===".equals(operator)) {
            switch (operator) {
            case "===":
                return arg1 == arg2;
            case "!==":
                return arg1 != arg2;
            default:
                throw IgnoredEventException.INSTANCE;
            }
        } else {
            arg1 = nullfilter(arg1);
            arg2 = protect(operator, arg2);
            // Detect if comparing an IP with a String, try to compare both as InetAddress
            arg2 = checkStringIp(arg1, arg2);
            arg1 = checkStringIp(arg2, arg1);
            ComparaisonClass arg1Class = ComparaisonClass.resolve(arg1);
            ComparaisonClass arg2Class = ComparaisonClass.resolve(arg2);
            if (arg1Class == ComparaisonClass.STRING && arg2Class == ComparaisonClass.NUMBER) {
                arg2Class = ComparaisonClass.STRING;
            }
            if (arg2Class == ComparaisonClass.STRING && arg1Class == ComparaisonClass.NUMBER) {
                arg1Class = ComparaisonClass.STRING;
            }
            if (arg1Class != arg2Class) {
                switch (operator) {
                case "==":
                    return false;
                case "!=":
                    return true;
                default:
                    throw IgnoredEventException.INSTANCE;
                }
            } else if ("==".equals(operator) || "!=".equals(operator)) {
                return compareBoolean(operator, arg1Class, arg1, arg2);
            } else {
                return compareOrdered(operator, arg1Class, arg1, arg2);
            }
        }
    }

    private static boolean compareBoolean(String operator, ComparaisonClass argClass, Object arg1, Object arg2) {
        boolean value;
        switch (argClass) {
        case NULL:
            value = true;
            break;
        case STRING:
            value = arg1.toString().equals(arg2.toString());
            break;
        case DATE:
            value = dateCompare(arg1, arg2) == 0;
            break;
        case NUMBER:
            value = numberCompare(arg1, arg2) == 0;
            break;
        case COLLECTION:
            value = DefaultTypeTransformation.compareEqual(arg1, arg2);
            break;
        case COMPARABLE:
            value = compareComparable(arg1, arg2) == 0;
            break;
        case IP_ADDRESS:
            value = ipCompare(arg1, arg2);
            break;
        default:
            value = arg1.equals(arg2);
        }
        return "==".equals(operator) == value;
    }

    private static Object compareOrdered(String operator, ComparaisonClass argClass, Object arg1, Object arg2) {
        int compare;
        switch (argClass) {
        case NULL:
            compare = 0;
            break;
        case DATE:
            compare = dateCompare(arg1, arg2);
            break;
        case NUMBER:
            compare = numberCompare(arg1, arg2);
            break;
        case STRING:
            compare = Helpers.NATURALSORTSTRING.compare(arg1.toString(), arg2.toString());
            break;
        case COMPARABLE:
            compare = compareComparable(arg1, arg2);
            break;
        default:
            throw IgnoredEventException.INSTANCE;
        }
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
            assert false : operator;
            throw IgnoredEventException.INSTANCE;
        }
    }

    private static boolean ipCompare(Object arg1, Object arg2) {
        if (arg1 instanceof InetAddress && arg2 instanceof InetAddress) {
            return arg1.equals(arg2);
        } else if (arg1 instanceof InetAddress && arg2 instanceof InetAddress[]) {
            InetAddress ip1 = (InetAddress) arg1;
            InetAddress[] ip2 = (InetAddress[]) arg2;
            return Arrays.asList(ip2).contains(ip1);
        } else if (arg2 instanceof InetAddress && arg1 instanceof InetAddress[]) {
            InetAddress ip1 = (InetAddress) arg2;
            InetAddress[] ip2 = (InetAddress[]) arg1;
            return Arrays.asList(ip2).contains(ip1);
        } else if (arg2 instanceof InetAddress[] && arg1 instanceof InetAddress[]) {
            Set<InetAddress> ip1 = Set.of((InetAddress[]) arg1);
            Set<InetAddress> ip2 = Set.of((InetAddress[]) arg2);
            return ip1.stream().anyMatch(ip2::contains);
        } else {
            // Not reachable
            assert false;
            return false;
        }
    }

    private static int dateCompare(Object arg1, Object arg2) {
        if (arg1 instanceof Date && arg2 instanceof TemporalAccessor) {
            try {
                long t1 = ((Date) arg1).getTime();
                long t2 = Instant.from((TemporalAccessor) arg2).toEpochMilli();
                return Long.compare(t1, t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg2 instanceof Date && arg1 instanceof TemporalAccessor) {
            try {
                long t2 = ((Date) arg2).getTime();
                long t1 = Instant.from((TemporalAccessor) arg1).toEpochMilli();
                return Long.compare(t1, t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg1 instanceof TemporalAccessor && arg2 instanceof TemporalAccessor) {
            // Groovy can't compare Instant and ZonedDateTime
            try {
                Instant t1 = Instant.from((TemporalAccessor) arg1);
                Instant t2 = Instant.from((TemporalAccessor) arg2);
                return t1.compareTo(t2);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
        } else if (arg1 instanceof Date && arg2 instanceof Date) {
            return ((Date) arg1).compareTo((Date) arg2);
        } else {
            assert false : "Not reachable";
            throw IgnoredEventException.INSTANCE;
        }
    }

    private static int numberCompare(Object arg1, Object arg2) {
        if (arg1 instanceof Number && arg2 instanceof Number) {
            return NumberMath.compareTo((Number) arg1, (Number) arg2);
        } else {
            assert false : "not reachable";
            throw IgnoredEventException.INSTANCE;
        }
    }

    private static int compareComparable(Object arg1, Object arg2) {
        if (arg1 instanceof Comparable && arg1.getClass().isAssignableFrom(arg2.getClass())) {
            return doComparableComparison(arg1, arg2);
        } else if (arg2 instanceof Comparable && arg2.getClass().isAssignableFrom(arg1.getClass())) {
            return doComparableComparison(arg2, arg1) * -1;
        } else {
            assert false : "not reachable";
            throw IgnoredEventException.INSTANCE;
        }
    }

    private static int doComparableComparison(Object c1, Object c2) {
        try {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            int value = ((Comparable) c1).compareTo(c2);
            return value;
        } catch (ClassCastException ex1) {
            try {
                // Perhaps groovy is smarter
                return DefaultTypeTransformation.compareTo(c1, c2);
            } catch (Exception ex2) {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public static Object regex(Object arg, String op, Pattern pattern) {
        if (arg == NullOrMissingValue.NULL || arg == null || arg instanceof Collection || arg instanceof java.util.Map || arg.getClass().isArray()) {
            return false;
        } else if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else {
            Matcher m = MATCHER_CACHE.apply(pattern);
            m.reset(arg.toString());
            if ("==~".equals(op)) {
                return m.matches();
            } else if ("=~".equals(op) && m.find()) {
                String[] groups = new String[m.groupCount() + 1];
                for (int i = 0; i < groups.length; i++) {
                    groups[i] = m.group(i);
                }
                return groups;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

    public static boolean asBoolean(Object arg) {
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

    public static Object groovyOperator(GroovyMethods operator, Object arg1) {
        arg1 = nullfilter(arg1);
        if (arg1 instanceof NullOrMissingValue) {
            throw IgnoredEventException.INSTANCE;
        }
        LoghubMetaClass<?> mc = (LoghubMetaClass<?>) registry.getMetaClass(arg1.getClass());
        return mc.invokeTypedMethod(arg1, operator);
    }

    public static Object groovyOperator(GroovyMethods operator, Object arg1, Object arg2) {
        if (arg1 instanceof NullOrMissingValue) {
            throw IgnoredEventException.INSTANCE;
        }
        LoghubMetaClass<?> mc = (LoghubMetaClass<?>) registry.getMetaClass(arg1.getClass());
        return mc.invokeTypedMethod(arg1, operator, arg2);
    }

    @SuppressWarnings("unchecked")
    public static <T> T convertObject(Class<T> clazz, Object value, Charset charset, ByteOrder byteOrder)
            throws UnknownHostException, InvocationTargetException {
        if (value == NullOrMissingValue.MISSING) {
            return (T) NullOrMissingValue.MISSING;
        } else if (value == null || value == NullOrMissingValue.NULL) {
            return null;
        } else if (clazz.isAssignableFrom(value.getClass())) {
            // Nothing to do, just return the value
            return (T) value;
        } else if (value instanceof byte[] && clazz == String.class) {
            return (T) new String((byte[]) value, charset);
        } else if (value instanceof byte[] && clazz == MacAddress.class) {
            return (T) new MacAddress((byte[]) value);
        } else if (value instanceof byte[] && InetAddress.class == clazz) {
                return (T) InetAddress.getByAddress((byte[]) value);
        } else if (value instanceof byte[]) {
            ByteBuffer buffer = ByteBuffer.wrap((byte[]) value);
            buffer.order(byteOrder);
            Object o;
            switch (clazz.getName()) {
            case "java.lang.Character":
                o = buffer.getChar();
                break;
            case "java.lang.Byte" :
                o = buffer.get();
                break;
            case "java.lang.Short":
                o = buffer.getShort();
                break;
            case "java.lang.Integer":
                o = buffer.getInt();
                break;
            case "java.lang.Long":
                o = buffer.getLong();
                break;
            case "java.lang.Float":
                o = buffer.getFloat();
                break;
            case "java.lang.Double":
                o = buffer.getDouble();
                break;
            default:
                throw IgnoredEventException.INSTANCE;
            }
            return (T) o;
        } else {
            String valueStr = value.toString();
            if (valueStr.isBlank()) {
                throw IgnoredEventException.INSTANCE;
            } else {
                Object o;
                switch (clazz.getName()) {
                case "java.lang.Integer":
                    o = Integer.valueOf(valueStr);
                    break;
                case "java.lang.Byte" :
                    o = Byte.valueOf(valueStr);
                    break;
                case "java.lang.Short":
                    o = Short.valueOf(valueStr);
                    break;
                case "java.lang.Long":
                    o = Long.valueOf(valueStr);
                    break;
                case "java.lang.Float":
                    o = Float.valueOf(valueStr);
                    break;
                case "java.lang.Double":
                    o = Double.valueOf(valueStr);
                    break;
                case "java.lang.Boolean":
                    o = Boolean.valueOf(valueStr);
                    break;
                case "java.net.InetAddress":
                    o = Helpers.parseIpAddress(valueStr);
                    break;
                 default:
                    o = BeansManager.constructFromString(clazz, valueStr);
                    break;
                }
                return (T) o;
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass, List<Object> args) {
        MetaClass mc = registry.getMetaClass(theClass);
        return (T) mc.invokeConstructor(args.toArray(Object[]::new));
    }

    private static Map<?, ?> copyMap(Map<?, ?> map) {
        return map.entrySet()
                  .stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, e -> deepCopy(e.getValue())));
    }

    public static Object deepCopy(Object v) {
        if (v == null) {
            return NullOrMissingValue.NULL;
        } else if (v instanceof Event) {
            Event e = (Event) v;
            return copyMap(e);
        } else if (v instanceof Map) {
            Map<?, ?> m = (Map<?, ?>) v;
            return copyMap(m);
        } else if (v instanceof List) {
            List<?> l = (List<?>) v;
            return l.stream()
                    .map(Expression::deepCopy)
                    .collect(Collectors.toCollection(ArrayList::new));
        } else if (v instanceof Set) {
            Set<?> s = (Set<?>) v;
            return s.stream()
                    .map(Expression::deepCopy)
                    .collect(Collectors.toCollection(HashSet::new));
        } else if (v.getClass().isArray()) {
            Class<?> c = v.getClass().getComponentType();
            int length = Array.getLength(v);
            Object newArray = Array.newInstance(c, length);
            for (int i = 0; i < length; i++) {
                Array.set(newArray, i, deepCopy(Array.get(v, i)));
            }
            return newArray;
        } else {
            return v;
        }
    }

    /**
     * Clear the compilation cache
     */
    public static void clearCache() {
        MATCHER_CLEAN.run();
    }

}
