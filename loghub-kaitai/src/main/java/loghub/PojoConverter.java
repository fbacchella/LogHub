package loghub;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import io.kaitai.struct.KaitaiStruct;

public class PojoConverter {

    public Map<String, Object> pojoToMap(Object pojo) throws Exception {
        if (pojo == null) {
            return null;
        }
        return (Map<String, Object>) convertObject(pojo, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private Object convertObject(Object obj, Set<Object> visited) throws Exception {
        if (obj == null || obj == NullOrMissingValue.NULL) {
            return null;
        }


        if (obj instanceof KaitaiStruct) {
            // Circular reference check
            if (visited.contains(obj) && ! (obj instanceof String)) {
                return "[Circular reference]";
            }
            visited.add(obj);
            return convertPojo(obj, visited);
        }

        // Arrays
        if (obj.getClass().isArray()) {
            return convertArray(obj, visited);
        }

        // Collections
        if (obj instanceof Collection) {
            return convertCollection((Collection<?>) obj, visited);
        }

        // Maps
        if (obj instanceof Map) {
            return convertMap((Map<?, ?>) obj, visited);
        }

        return obj;
    }

    private Map<String, Object> convertPojo(Object pojo, Set<Object> visited) throws Exception {
        Map<String, Object> map = new LinkedHashMap<>();

        Class<?> clazz = pojo.getClass();
        Method[] methods = clazz.getMethods();

        for (Method method : methods) {
            // Check that it's a public method
            if (!Modifier.isPublic(method.getModifiers())) {
                continue;
            }

            // Check that there are no parameters
            if (method.getParameterCount() != 0) {
                continue;
            }

            // Check that there is a return type (not void)
            if (method.getReturnType() == void.class ||
                        method.getReturnType() == Void.class) {
                continue;
            }

            // Exclude methods from Object
            if (isMethodFromParentOrKaitaiPrivate(method)) {
                continue;
            }

            String propertyName = extractPropertyName(method);

            try {
                Object value = method.invoke(pojo);
                map.put(propertyName, convertObject(value, visited));
            } catch (Exception e) {
                map.put(propertyName, "[Error: " + e.getMessage() + "]");
            }
        }

        return map;
    }

    private String extractPropertyName(Method method) {
        String methodName = method.getName();

        // Getter convention: getXxx -> xxx
        if (methodName.startsWith("get") && methodName.length() > 3) {
            return decapitalize(methodName.substring(3));
        }

        // Boolean getter convention: isXxx -> xxx
        if (methodName.startsWith("is") && methodName.length() > 2 &&
                    (method.getReturnType() == boolean.class ||
                             method.getReturnType() == Boolean.class)) {
            return decapitalize(methodName.substring(2));
        }

        // Boolean getter convention: hasXxx -> xxx
        if (methodName.startsWith("has") && methodName.length() > 3 &&
                    (method.getReturnType() == boolean.class ||
                             method.getReturnType() == Boolean.class)) {
            return decapitalize(methodName.substring(3));
        }

        // Any other method: use the name as is
        return methodName;
    }

    private String decapitalize(String string) {
        if (string == null || string.isEmpty()) {
            return string;
        }

        // Handle acronyms (e.g., getURL -> url, not uRL)
        if (string.length() > 1 &&
                    Character.isUpperCase(string.charAt(1))) {
            return string;
        }

        char[] chars = string.toCharArray();
        chars[0] = Character.toLowerCase(chars[0]);
        return new String(chars);
    }

    private static final Set<String> EXCLUDED_METHODS = Set.of(
            "getClass", "hashCode", "toString", "equals", "notify",
            "notifyAll", "wait", "clone", "finalize"
    );

    private boolean isMethodFromParentOrKaitaiPrivate(Method method) {
        String methodName = method.getName();

        // Methods inherited from Object to exclude
        return EXCLUDED_METHODS.contains(methodName) ||
                       methodName.startsWith("_");
    }

    private List<Object> convertArray(Object array, Set<Object> visited) throws Exception {
        int length = Array.getLength(array);
        List<Object> list = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            Object element = Array.get(array, i);
            list.add(convertObject(element, visited));
        }

        return list;
    }

    private List<Object> convertCollection(Collection<?> collection, Set<Object> visited) throws Exception {
        List<Object> list = new ArrayList<>(collection.size());

        for (Object element : collection) {
            list.add(convertObject(element, visited));
        }

        return list;
    }

    private Map<Object, Object> convertMap(Map<?, ?> sourceMap, Set<Object> visited) throws Exception {
        Map<Object, Object> resultMap = new LinkedHashMap<>();

        for (Map.Entry<?, ?> entry : sourceMap.entrySet()) {
            Object key = convertObject(entry.getKey(), visited);
            Object value = convertObject(entry.getValue(), visited);
            resultMap.put(key, value);
        }

        return resultMap;
    }

}
