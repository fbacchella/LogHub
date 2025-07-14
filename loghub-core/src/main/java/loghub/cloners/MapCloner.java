package loghub.cloners;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class MapCloner {

    enum MAPCONSTRUCTOR {
        EMPTYMAP(n -> Map.of()),
        IDENTITYHASHMAP(n -> new IdentityHashMap<>(n * 2)),
        CONCURRENTHASHMAP(n -> new ConcurrentHashMap<>(n * 2)),
        HASHMAP(n -> new HashMap<>(n * 2)),
        LINKEDHASHMAP(n -> new LinkedHashMap<>(n * 2));

        private final Function<Integer, Map<Object, Object>> constructor;

        MAPCONSTRUCTOR(Function<Integer, Map<Object, Object>> constructor) {
            this.constructor = constructor;
        }

        Map<Object, Object> generate(int n) {
            return constructor.apply(n);
        }
    }
    static final MAPCONSTRUCTOR[] MAPCONSTRUCTORS = MAPCONSTRUCTOR.values();

    static final Map<Class<? extends Map>, MAPCONSTRUCTOR> MAP_MAPPING = Map.ofEntries(
            Map.entry(Map.of(1, 2).getClass(), MAPCONSTRUCTOR.HASHMAP),
            Map.entry(IdentityHashMap.class, MAPCONSTRUCTOR.IDENTITYHASHMAP),
            Map.entry(ConcurrentHashMap.class, MAPCONSTRUCTOR.CONCURRENTHASHMAP),
            Map.entry(HashMap.class, MAPCONSTRUCTOR.HASHMAP),
            Map.entry(LinkedHashMap.class, MAPCONSTRUCTOR.LINKEDHASHMAP)
    );

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> clone(Map<K, V> oldMap) {
        Map<K, V> newMap = (Map<K, V>) MAP_MAPPING.get(oldMap.getClass()).generate(oldMap.size());
        oldMap.forEach((k, v) -> newMap.put(DeepCloner.clone(k), DeepCloner.clone(v)));
        return newMap;
    }

}
