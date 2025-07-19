package loghub.cloners;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.SocketAddress;
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

    public static <T> void register(Class<T> clazz, Cloner<?> customCloner) {
        faster.put(clazz, customCloner);
    }
    public static void registerImmutable(Class<?> clazz) {
        immutables.add(clazz);
    }

    static boolean isImmutable(Class<?> oClass) {
        return SocketAddress.class.isAssignableFrom(oClass)
                       || InetAddress.class.isAssignableFrom(oClass)
                       || oClass.isEnum()
                       || immutables.contains(oClass)
                       || oClass.isAnnotationPresent(Immutable.class);
    }

    static boolean isImmutable(Object o) {
        Class<?> oClass = o.getClass();
        return o instanceof  SocketAddress
               || o instanceof InetAddress
               || oClass.isEnum()
               || immutables.contains(oClass)
               || singlotons.contains(o)
               || oClass.isAnnotationPresent(Immutable.class);
    }

    @SuppressWarnings("unchecked")
    static <T> T tryClone(T obj) {
        try {
            Method cloneMethod = obj.getClass().getMethod("clone");
            return (T) cloneMethod.invoke(obj);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            return CloneOpaque.clone(obj);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T clone(T o) {
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
        } else if (isImmutable(o)) {
            return o;
        } else if (faster.containsKey(o.getClass())) {
            return ((Cloner<T>) faster.get(o.getClass())).clone(o);
        } else if (o instanceof Map) {
            return (T) MapCloner.clone((Map<?, ?>) o);
        } else if (o instanceof Collection) {
            return (T) CloneCollection.clone((List<Object>) o);
        } else if (o.getClass().isArray()) {
            return (T) CloneArray.clone(o);
        } else if (o instanceof Cloneable) {
            return tryClone(o);
        } else {
            return CloneOpaque.clone(o);
        }
    }

    private DeepCloner() {}

}
