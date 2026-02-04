package loghub.cloners;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZonedDateTime;
import java.time.chrono.HijrahDate;
import java.time.chrono.JapaneseDate;
import java.time.chrono.MinguoDate;
import java.time.chrono.ThaiBuddhistDate;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.ssl.SSLSession;

import loghub.ConnectionContext;
import loghub.NullOrMissingValue;

public class DeepCloner {

    static final Set<Object> singlotons = new HashSet<>();
    static final Map<Class<?>, Cloner<?>> faster = new HashMap<>();
    private static final Set<Class<?>> immutables = new HashSet<>();
    static {
        immutables.add(Instant.class);
        immutables.add(Period.class);
        immutables.add(Duration.class);
        immutables.add(HijrahDate.class);
        immutables.add(JapaneseDate.class);
        immutables.add(LocalDate.class);
        immutables.add(MinguoDate.class);
        immutables.add(ZonedDateTime.class);
        immutables.add(OffsetDateTime.class);
        immutables.add(OffsetTime.class);
        immutables.add(ThaiBuddhistDate.class);
        immutables.add(Year.class);
        immutables.add(YearMonth.class);
        immutables.add(LocalDateTime.class);
        immutables.add(LocalTime.class);
        immutables.add(Character.class);
        immutables.add(String.class);
        immutables.add(Boolean.class);
        immutables.add(Byte.class);
        immutables.add(Short.class);
        immutables.add(Integer.class);
        immutables.add(Long.class);
        immutables.add(Float.class);
        immutables.add(Double.class);
        immutables.add(UUID.class);
        immutables.add(Class.class);

        faster.put(EnumMap.class, o -> EnumMapCloner.clone((EnumMap<? extends Enum<?>, ?>) o));

        singlotons.add(Map.of());
        singlotons.add(Collections.emptyMap());
        singlotons.add(Set.of());
        singlotons.add(Collections.emptySet());
        singlotons.add(List.of());
        singlotons.add(Collections.emptyList());
        singlotons.add(Collections.emptyEnumeration());
        singlotons.add(Collections.emptyIterator());
        singlotons.add(Collections.emptyNavigableMap());
        singlotons.add(ConnectionContext.EMPTY);
        singlotons.add(Boolean.TRUE);
        singlotons.add(Boolean.FALSE);
        singlotons.add(NullOrMissingValue.NULL);
        singlotons.add(NullOrMissingValue.MISSING);
    }

    public static <T> void register(Class<T> clazz, Cloner<T> customCloner) {
        faster.put(clazz, customCloner);
    }
    public static void registerImmutable(Class<?> clazz) {
        immutables.add(clazz);
    }

    private static final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
    private static final MethodType CLONE_MT = MethodType.methodType(Object.class);
    private static final ConcurrentHashMap<Class<?>, Cloner<?>> CLONERS_MAP = new ConcurrentHashMap<>();

    static boolean isImmutable(Class<?> oClass) {
        return oClass.isEnum()
               || oClass.isRecord()
               || immutables.contains(oClass)
               || oClass.isAnnotationPresent(Immutable.class)
               || SocketAddress.class.isAssignableFrom(oClass)
               || InetAddress.class.isAssignableFrom(oClass);
    }

    static boolean isImmutable(Object o) {
        return o instanceof SocketAddress
               || o instanceof InetAddress
               || o == null
               || isImmutable(o.getClass())
               || singlotons.contains(o);
    }

    @SuppressWarnings("unchecked")
    static <T> T tryClone(T obj) throws NotClonableException {
        Cloner<T> lambda = (Cloner<T>) CLONERS_MAP.computeIfAbsent(obj.getClass(), DeepCloner::resolveHandle);
        return lambda.clone(obj);
    }

    @SuppressWarnings("unchecked")
    static <T> T cloneWithHandler(MethodHandle mh, T o) throws NotClonableException {
        try {
            return (T) mh.invoke(o);
        } catch (Throwable e) {
            throw new NotClonableException(o.getClass(), e);
        }
    }

    private static <T> Cloner<T> resolveHandle(Class<T> clazz) {
        try {
            MethodHandle mh = lookup.findVirtual(clazz, "clone", CLONE_MT);
            return o -> cloneWithHandler(mh, o);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            return CloneOpaque::clone;
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T clone(T o) throws NotClonableException {
        if (o == null) {
            return null;
        } else if (o instanceof Byte) {
            return o;
        } else if (o instanceof Short) {
            return o;
        } else if (o instanceof Integer) {
            return o;
        } else if (o instanceof Long) {
            return o;
        } else if (o instanceof Float) {
            return o;
        } else if (o instanceof Double) {
            return o;
        } else if (o instanceof Character) {
            return o;
        } else if (o instanceof ByteBuffer bb && bb.isReadOnly()) {
            return o;
        } else if (isImmutable(o)) {
            return o;
        } else if (faster.containsKey(o.getClass())) {
            return ((Cloner<T>) faster.get(o.getClass())).clone(o);
        } else if (o instanceof Map<?, ?> m) {
            return (T) MapCloner.clone(m);
        } else if (o instanceof Collection<?> c) {
            return (T) CloneCollection.clone((Collection<Object>) c);
        } else if (o.getClass().isArray()) {
            return (T) CloneArray.clone(o);
        } else if (o instanceof SSLSession ssls) {
            // SSLSession is an interface, not detected in faster
            return (T) SSLSessionCloned.clone(ssls);
        } else if (o instanceof Cloneable) {
            return tryClone(o);
        } else {
            return CloneOpaque.clone(o);
        }
    }

    private DeepCloner() {}

}
