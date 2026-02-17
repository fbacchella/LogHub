package loghub.processors;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import loghub.BuilderClass;
import loghub.NullOrMissingValue;
import loghub.ProcessorException;
import loghub.events.Event;
import lombok.Setter;

/**
 * A processor that regroups values from a Map where data is exploded across several arrays or lists.
 * <p>
 * This processor performs a gathering or transposition operation. It takes a Map where each key
 * points to a list of values (or a single value) and transforms it into a list of Maps. Each Map
 * in the resulting list represents one row of data, combining elements that were at the same
 * index in the original lists.
 * </p>
 * <p>
 * For example, an input Map like:
 * <pre>
 * {
 *   "host": ["host1", "host2"],
 *   "status": [200, 404]
 * }
 * </pre>
 * will be transformed into:
 * <pre>
 * [
 *   {"host": "host1", "status": 200},
 *   {"host": "host2", "status": 404}
 * ]
 * </pre>
 * </p>
 * <p>
 * Processing details:
 * <ul>
 *   <li>The input value must be a {@link java.util.Map}, otherwise it returns {@link RUNSTATUS#NOSTORE}.</li>
 *   <li>Each value in the Map is converted to a {@link java.util.List}. Supported types include
 *       {@link java.util.Collection}, arrays, {@link java.lang.Iterable}, and {@link java.util.Iterator}.</li>
 *   <li>Single values (non-iterable or null) are treated as single-element lists.</li>
 *   <li>If lists have unequal lengths, missing values are padded with {@link loghub.NullOrMissingValue#NULL}
 *       to ensure all resulting Maps have the same keys.</li>
 * </ul>
 * </p>
 */
@BuilderClass(Gatherer.Builder.class)
public class Gatherer extends FieldsProcessor {

    @Setter
    public static class Builder extends FieldsProcessor.Builder<Gatherer> {
        public Gatherer build() {
            return new Gatherer(this);
        }
    }

    public Gatherer(Builder builder) {
        super(builder);
    }

    @Override
    public Object fieldFunction(Event event, Object value) throws ProcessorException {
        if (value instanceof Map<?, ?> map) {
            Map<Object, List<Object>> listsMap = getListsMap(map);
            int maxLen = listsMap.values().stream()
                    .mapToInt(List::size)
                    .max()
                    .orElse(0);

            List<Map<Object, Object>> result = new ArrayList<>(maxLen);
            for (int i = 0; i < maxLen; i++) {
                Map<Object, Object> newMap = HashMap.newHashMap(map.size());
                for (Map.Entry<Object, List<Object>> entry : listsMap.entrySet()) {
                    List<Object> list = entry.getValue();
                    if (i < list.size()) {
                        newMap.put(entry.getKey(), list.get(i));
                    } else {
                        newMap.put(entry.getKey(), NullOrMissingValue.NULL);
                    }
                }
                result.add(newMap);
            }
            return result;
        } else {
            return RUNSTATUS.NOSTORE;
        }
    }

    private Map<Object, List<Object>> getListsMap(Map<?, ?> map) {
        Map<Object, List<Object>> listsMap = HashMap.newHashMap(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object val = entry.getValue();
            List<Object> list = switch (val) {
                case null -> Collections.singletonList(NullOrMissingValue.NULL);
                case List<?> l -> (List<Object>) l;
                case Collection<?> c -> new ArrayList<>(c);
                case Object a when a.getClass().isArray() -> {
                    int length = Array.getLength(a);
                    List<Object> l = new ArrayList<>(length);
                    for (int i = 0; i < length; i++) {
                        l.add(Array.get(a, i));
                    }
                    yield l;
                }
                case Iterable<?> i -> {
                    List<Object> l = new ArrayList<>();
                    for (Object o : i) {
                        l.add(o);
                    }
                    yield l;
                }
                case Iterator<?> it -> {
                    List<Object> l = new ArrayList<>();
                    while (it.hasNext()) {
                        l.add(it.next());
                    }
                    yield l;
                }
                default -> Collections.singletonList(val);
            };
            listsMap.put(entry.getKey(), list);
        }
        return listsMap;
    }

}
