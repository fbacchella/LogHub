package loghub.cloners;

import java.util.EnumMap;
import java.util.Map;

public class EnumMapCloner {

    @SuppressWarnings({"unchecked", "java:S1452", "java:S1319"})
    public static <T extends Enum<T>> EnumMap<T, ?> clone(EnumMap<T, ?> map) throws NotClonableException {
        if (map.isEmpty()) {
            return new EnumMap<>(map);
        } else {
            Class<T> enumClass = (Class<T>) map.entrySet().iterator().next().getKey().getClass();
            EnumMap<T, Object> newMap = new EnumMap<>(enumClass);
            for (Map.Entry<T, ?> e: map.entrySet()) {
                newMap.put(e.getKey(), DeepCloner.clone(e.getValue()));
            }
            return newMap;
        }
    }

    private EnumMapCloner() {
    }

}
