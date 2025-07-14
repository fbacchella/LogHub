package loghub.cloners;

import java.util.EnumMap;

public class EnumMapCloner {

    @SuppressWarnings({"unchecked", "java:S1452", "java:S1319"})
    public static <T extends Enum<T>> EnumMap<T, ?> clone(EnumMap<T, ?> map) {
        if (map.isEmpty()) {
            return new EnumMap<>(map);
        } else {
            Class<T> enumClass = (Class<T>) map.entrySet().iterator().next().getKey().getClass();
            EnumMap<T, Object> newMap = new EnumMap<>(enumClass);
            map.forEach((k, v) -> newMap.put(k, DeepCloner.clone(v)));
            return newMap;
        }
    }

    private EnumMapCloner() {
    }

}
