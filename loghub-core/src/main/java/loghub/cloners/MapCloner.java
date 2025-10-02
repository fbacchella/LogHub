package loghub.cloners;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class MapCloner {

    enum MAPCONSTRUCTOR {
        EMPTYMAP(n -> Map.of()),
        IDENTITYHASHMAP(n -> new IdentityHashMap<>(n * 2)),
        CONCURRENTHASHMAP(n -> new ConcurrentHashMap<>(n * 2)),
        HASHMAP(HashMap::newHashMap),
        LINKEDHASHMAP(LinkedHashMap::newHashMap),
        HASHTABLE(n -> new Hashtable<>(n * 2));

        private final Function<Integer, Map<Object, Object>> constructor;

        MAPCONSTRUCTOR(Function<Integer, Map<Object, Object>> constructor) {
            this.constructor = constructor;
        }

        Map<Object, Object> generate(int n) {
            return constructor.apply(n);
        }
    }

    static final Map<Class<? extends Map>, MAPCONSTRUCTOR> MAP_MAPPING = Map.ofEntries(
        Map.entry(Map.of(1, 2).getClass(), MAPCONSTRUCTOR.HASHMAP),
        Map.entry(Map.of(1, 2, 3, 4).getClass(), MAPCONSTRUCTOR.HASHMAP),
        Map.entry(IdentityHashMap.class, MAPCONSTRUCTOR.IDENTITYHASHMAP),
        Map.entry(ConcurrentHashMap.class, MAPCONSTRUCTOR.CONCURRENTHASHMAP),
        Map.entry(HashMap.class, MAPCONSTRUCTOR.HASHMAP),
        Map.entry(LinkedHashMap.class, MAPCONSTRUCTOR.LINKEDHASHMAP)
    );

    private static final Map<Class<?>, UnaryOperator<? extends Map<?, ?>>> cloneCache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    private static <K, V> UnaryOperator<Map<K, V>> findCreator(Class<Map<K, V>> c, Function<Class<? extends Map<K, V>>, UnaryOperator<Map<K, V>>> finder) {
        return (UnaryOperator<Map<K, V>>) cloneCache.computeIfAbsent(
                c,
                cls -> finder.apply((Class<? extends Map<K, V>>) cls)
        );
    }

    @SuppressWarnings("unchecked")
    private static <K, V> UnaryOperator<Map<K, V>> findCloner(Class<? extends Map<K, V>> c) {
        try {
            Method m = c.getMethod("clone"); // ou le nom correct du cloner
            return map -> {
                try {
                    return (Map<K, V>) m.invoke(map);
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(e);
                } catch (InvocationTargetException e) {
                    throw new IllegalStateException(e.getCause());
                }
            };
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Not a cloneable object: " + c.getName(), e);
        }
    }

    private static <K, V> UnaryOperator<Map<K, V>> findConstructor(Class<? extends Map<K, V>> cls) {
        try {
            Constructor<? extends Map<K, V>> c = cls.getConstructor(Integer.TYPE);
            return map -> {
                try {
                    return c.newInstance(map.size() * 2);
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new IllegalStateException(e);
                  } catch (InvocationTargetException e) {
                    throw new IllegalStateException(e.getCause());
                }
            };
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Not a instantiable object: " + cls.getName(), e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> clone(Map<K, V> oldMap) throws NotClonableException {
        Map<K, V> newMap;
        if (MapCloner.MAP_MAPPING.containsKey(oldMap.getClass())) {
            newMap = (Map<K, V>) MAP_MAPPING.get(oldMap.getClass()).generate(oldMap.size());
        } else if (oldMap instanceof Cloneable) {
            // clone of a map return a shallow copy, protect from that
            newMap = findCreator((Class<Map<K, V>>)oldMap.getClass(), MapCloner::findCloner).apply(oldMap);
        } else {
            newMap = findCreator((Class<Map<K, V>>)oldMap.getClass(), MapCloner::findConstructor).apply(oldMap);
        }
        for (Map.Entry<K, V> e: oldMap.entrySet()) {
            newMap.put(DeepCloner.clone(e.getKey()), DeepCloner.clone(e.getValue()));
        }
        return newMap;
    }

}
