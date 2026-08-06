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
import java.util.stream.IntStream;
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
                return switch (theClass) {
                    case Class<?> c when metaClassFactories.containsKey(c) -> doCreate(c, c);
                    case Class<?> c when Temporal.class.isAssignableFrom(c) -> doCreate(Temporal.class, c);
                    case Class<?> c when Number.class.isAssignableFrom(c) -> doCreate(Number.class, c);
                    case Class<?> c when Collection.class.isAssignableFrom(c) -> doCreate(Collection.class, c);
                    case Class<?> c when CharSequence.class.isAssignableFrom(c) -> doCreate(CharSequence.class, c);
                    case Class<?> c when c.isArray() -> doCreate(Collection.class, c);
                    default -> {
                        logger.debug("Creating unhandled MetaClass {}", theClass::getName);
                        yield doCreate(Object.class, theClass);
                    }
                };
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
        this.source = switch (literal) {
            case String s -> String.format("\"%s\"", s);
            case Character c -> String.format("'%s'", c);
            case null -> "null";
            case NullOrMissingValue n when n == NullOrMissingValue.NULL -> "null";
            default -> literal.toString();
        };
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
            return switch (method) {
                case "trim" -> nullarg ? NullOrMissingValue.NULL : arg.toString().trim();
                case "capitalize" -> nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.capitalize(arg.toString());
                case "uncapitalize" ->
                        nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.uncapitalize(arg.toString());
                case "isBlank" -> nullarg || StringGroovyMethods.isAllWhitespace(arg.toString());
                case "normalize" -> nullarg ? NullOrMissingValue.NULL : StringGroovyMethods.normalize(arg.toString());
                case "lowercase" -> nullarg ? NullOrMissingValue.NULL : arg.toString().toLowerCase();
                case "uppercase" -> nullarg ? NullOrMissingValue.NULL : arg.toString().toUpperCase();
                default -> {
                    // Can’t be reached
                    assert false : method;
                    throw IgnoredEventException.INSTANCE;
                }
            };
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
            if (arg2 instanceof Collection<?> list) {
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
        boolean result = switch (obj) {
            case null -> false;
            case NullOrMissingValue n -> false;
            default -> clazz.isAssignableFrom(obj.getClass());
        };
        return negated != result;
    }

    public static boolean in(String cmd, Object obj1, Object obj2) {
        boolean result = switch (obj1) {
            case NullOrMissingValue n when n == NullOrMissingValue.MISSING -> throw IgnoredEventException.INSTANCE;
            case null -> switch (obj2) {
                case NullOrMissingValue n2 when n2 == NullOrMissingValue.MISSING -> throw IgnoredEventException.INSTANCE;
                case null -> true;
                case NullOrMissingValue n2 when n2 == NullOrMissingValue.NULL -> true;
                default -> false;
            };
            case NullOrMissingValue n when n == NullOrMissingValue.NULL -> switch (obj2) {
                case NullOrMissingValue n2 when n2 == NullOrMissingValue.MISSING -> throw IgnoredEventException.INSTANCE;
                case null -> true;
                case NullOrMissingValue n2 when n2 == NullOrMissingValue.NULL -> true;
                default -> false;
            };
            default -> switch (obj2) {
                case NullOrMissingValue n2 when n2 == NullOrMissingValue.MISSING -> throw IgnoredEventException.INSTANCE;
                case Collection<?> c -> c.contains(obj1);
                case Object arr when arr.getClass().isArray() -> DefaultTypeTransformation.primitiveArrayToList(arr).contains(obj1);
                case CharSequence cs2 when (obj1 instanceof CharSequence || obj1 instanceof Character) -> cs2.toString().contains(obj1.toString());
                default -> false;
            };
        };
        return cmd.startsWith("!") != result;
    }

    public static Object newCollection(String collectionType) {
        return switch (collectionType) {
            case "set" -> new LinkedHashSet<>();
            case "list" -> new ArrayList<>();
            default -> {
                // Can’t be reached
                assert false : collectionType;
                throw IgnoredEventException.INSTANCE;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> Object asCollection(String collectionType, Object argument) {
        return switch (collectionType) {
            case "set" -> {
                if (argument instanceof Set) {
                    yield argument;
                } else if (argument instanceof Collection) {
                    yield new LinkedHashSet<>((Collection<T>) argument);
                } else if (argument.getClass().isArray()) {
                    yield new LinkedHashSet<T>(DefaultTypeTransformation.primitiveArrayToList(argument));
                } else {
                    yield new LinkedHashSet<>(Set.of((T) argument));
                }
            }
            case "list" -> {
                if (argument instanceof List) {
                    yield argument;
                } else if (argument instanceof Collection) {
                    yield new ArrayList<>((Collection<T>) argument);
                } else if (argument.getClass().isArray()) {
                    yield new ArrayList<T>(DefaultTypeTransformation.primitiveArrayToList(argument));
                } else {
                    yield new ArrayList<>(List.of((T) argument));
                }
            }
            default -> {
                // Can’t be reached
                assert false : collectionType;
                throw IgnoredEventException.INSTANCE;
            }
        };
    }

    public static Object getIterableIndex(Object iterable, int index) {
        return switch (iterable) {
            case null -> NullOrMissingValue.NULL;
            case NullOrMissingValue n when n == NullOrMissingValue.NULL -> NullOrMissingValue.NULL;
            case Object[] a -> {
                int pos = index >= 0 ? index : (a.length + index);
                if (a.length > pos) {
                    yield a[pos];
                } else {
                    throw IgnoredEventException.INSTANCE;
                }
            }
            case List<?> l -> {
                int pos = index >= 0 ? index : (l.size() + index);
                if (l.size() > pos) {
                    yield l.get(pos);
                } else {
                    throw IgnoredEventException.INSTANCE;
                }
            }
            case NullOrMissingValue n when n == NullOrMissingValue.MISSING -> throw IgnoredEventException.INSTANCE;
            default -> throw new IllegalArgumentException("Array operation on not iterable object");
        };
    }

    public static boolean isEmpty(Object arg) {
        if (arg == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (arg == null || arg == NullOrMissingValue.NULL) {
            return true;
        } else if (arg instanceof String s) {
            return s.isEmpty();
        } else if (arg instanceof Collection<?> c) {
            return c.isEmpty();
        } else if (arg instanceof java.util.Map<?, ?> m) {
            return m.isEmpty();
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
            return switch (operator) {
                case "===" -> arg1 == arg2;
                case "!==" -> arg1 != arg2;
                default -> throw IgnoredEventException.INSTANCE;
            };
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
                return switch (operator) {
                    case "==" -> false;
                    case "!=" -> true;
                    default -> throw IgnoredEventException.INSTANCE;
                };
            } else if ("==".equals(operator) || "!=".equals(operator)) {
                return compareBoolean(operator, arg1Class, arg1, arg2);
            } else {
                return compareOrdered(operator, arg1Class, arg1, arg2);
            }
        }
    }

    private static boolean compareBoolean(String operator, ComparaisonClass argClass, Object arg1, Object arg2) {
        boolean value = switch (argClass) {
            case NULL -> true;
            case STRING -> arg1.toString().equals(arg2.toString());
            case DATE -> dateCompare(arg1, arg2) == 0;
            case NUMBER -> numberCompare(arg1, arg2) == 0;
            case COLLECTION -> DefaultTypeTransformation.compareEqual(arg1, arg2);
            case COMPARABLE -> compareComparable(arg1, arg2) == 0;
            case IP_ADDRESS -> ipCompare(arg1, arg2);
            default -> arg1.equals(arg2);
        };
        return "==".equals(operator) == value;
    }

    private static Object compareOrdered(String operator, ComparaisonClass argClass, Object arg1, Object arg2) {
        int compare = switch (argClass) {
            case NULL -> 0;
            case DATE -> dateCompare(arg1, arg2);
            case NUMBER -> numberCompare(arg1, arg2);
            case STRING -> Helpers.NATURALSORTSTRING.compare(arg1.toString(), arg2.toString());
            case COMPARABLE -> compareComparable(arg1, arg2);
            default -> throw IgnoredEventException.INSTANCE;
        };
        return switch (operator) {
            case "<" -> compare < 0;
            case ">" -> compare > 0;
            case ">=" -> compare >= 0;
            case "<=" -> compare <= 0;
            case "<=>" -> compare;
            default -> {
                assert false : operator;
                throw IgnoredEventException.INSTANCE;
            }
        };
    }

    private static boolean ipCompare(Object arg1, Object arg2) {
        if (arg1 instanceof InetAddress && arg2 instanceof InetAddress) {
            return arg1.equals(arg2);
        } else if (arg1 instanceof InetAddress ip1 && arg2 instanceof InetAddress[] ip2) {
            return Arrays.asList(ip2).contains(ip1);
        } else if (arg2 instanceof InetAddress ip1 && arg1 instanceof InetAddress[] ip2) {
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
        } else if (arg instanceof Boolean b) {
            return b;
        } else if (arg instanceof Float || arg instanceof Double || arg instanceof BigDecimal) {
            return ((Number) arg).doubleValue() != 0;
        } else if (arg instanceof Number n) {
            return n.longValue() != 0;
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
            throws InvocationTargetException {
        try {
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
                Object o = switch (clazz.getName()) {
                    case "java.lang.Character" -> buffer.getChar();
                    case "java.lang.Byte" -> buffer.get();
                    case "java.lang.Short" -> buffer.getShort();
                    case "java.lang.Integer" -> buffer.getInt();
                    case "java.lang.Long" -> buffer.getLong();
                    case "java.lang.Float" -> buffer.getFloat();
                    case "java.lang.Double" -> buffer.getDouble();
                    default -> throw IgnoredEventException.INSTANCE;
                };
                return (T) o;
            } else if (value instanceof Number n && Number.class.isAssignableFrom(clazz)) {
                Object o = switch (clazz.getName()) {
                    case "java.lang.Integer" -> n.intValue();
                    case "java.lang.Byte" -> n.byteValue();
                    case "java.lang.Short" -> n.shortValue();
                    case "java.lang.Long" -> n.longValue();
                    case "java.lang.Float" -> n.floatValue();
                    case "java.lang.Double" -> n.doubleValue();
                    default -> throw IgnoredEventException.INSTANCE;
                };
                return (T) o;
            } else {
                assert ! (value instanceof Number && Number.class.isAssignableFrom(clazz));
                String valueStr = value.toString();
                if (valueStr.isBlank()) {
                    throw IgnoredEventException.INSTANCE;
                } else {
                    Object o = switch (clazz.getName()) {
                        case "java.lang.Integer" -> Integer.valueOf(valueStr);
                        case "java.lang.Byte" -> Byte.valueOf(valueStr);
                        case "java.lang.Short" -> Short.valueOf(valueStr);
                        case "java.lang.Long" -> Long.valueOf(valueStr);
                        case "java.lang.Float" -> Float.valueOf(valueStr);
                        case "java.lang.Double" -> Double.valueOf(valueStr);
                        case "java.lang.Boolean" -> Boolean.valueOf(valueStr);
                        case "java.net.InetAddress" -> Helpers.parseIpAddress(valueStr);
                        default -> BeansManager.constructFromString(clazz, valueStr);
                    };
                    return (T) o;
                }
            }
        } catch (UnknownHostException | RuntimeException ex) {
            throw new InvocationTargetException(ex);
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
        return switch (v) {
            case null -> NullOrMissingValue.NULL;
            case Map<?, ?> m -> copyMap(m);
            case List<?> l -> l.stream()
                               .map(Expression::deepCopy)
                               .collect(Collectors.toCollection(ArrayList::new));
            case Set<?> s -> s.stream()
                              .map(Expression::deepCopy)
                              .collect(Collectors.toCollection(HashSet::new));
            case Object arr when arr.getClass().isArray() -> {
                Class<?> c = arr.getClass().getComponentType();
                int length = Array.getLength(arr);
                Object newArray = Array.newInstance(c, length);
                for (int i = 0; i < length; i++) {
                    Array.set(newArray, i, deepCopy(Array.get(arr, i)));
                }
                yield newArray;
            }
            default -> v;
        };
    }

    public static Object flatten(Object value) {
        if (value == null || value == NullOrMissingValue.NULL) {
            return NullOrMissingValue.NULL;
        } else if (value == NullOrMissingValue.MISSING) {
            throw IgnoredEventException.INSTANCE;
        } else if (! (value instanceof Collection)  && (! value.getClass().isArray()) && (! (value instanceof Stream<?>))) {
            return value;
        } else {
            Stream<?> flatStream = toStream(value);
            Collection<?> c =  switch (value) {
                case Set<?> s -> flatStream.collect(Collectors.toSet());
                default -> flatStream.toList();
            };
            if (c.size() == 1) {
                return c.iterator().next();
            } else {
                return c;
            }
        }
    }

    private static Stream<?> toStream(Object value) {
        return switch (value) {
            case null -> Stream.of(NullOrMissingValue.NULL);
            case NullOrMissingValue n -> n == NullOrMissingValue.MISSING ? Stream.empty() : Stream.of(NullOrMissingValue.NULL);
            case Collection<?> c -> c.stream().filter(e -> e != NullOrMissingValue.MISSING).flatMap(Expression::toStream);
            case Object[] arr -> Arrays.stream(arr).flatMap(Expression::toStream);
            case int[] arr -> Arrays.stream(arr).boxed();
            case long[] arr -> Arrays.stream(arr).boxed();
            case double[] arr -> Arrays.stream(arr).boxed();
            case byte[] arr -> IntStream.range(0, arr.length).mapToObj(i -> arr[i]);
            case short[] arr -> IntStream.range(0, arr.length).mapToObj(i -> arr[i]);
            case float[] arr -> IntStream.range(0, arr.length).mapToObj(i -> arr[i]);
            case char[] arr -> IntStream.range(0, arr.length).mapToObj(i -> arr[i]);
            case boolean[] arr -> IntStream.range(0, arr.length).mapToObj(i -> arr[i]);
            case Stream<?> s -> s.filter(e -> e != NullOrMissingValue.MISSING).flatMap(Expression::toStream);
            default -> Stream.of(value);
        };
    }

    /**
     * Clear the compilation cache
     */
    public static void clearCache() {
        MATCHER_CLEAN.run();
    }

}
