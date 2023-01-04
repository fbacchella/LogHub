package loghub.configuration;

import java.beans.FeatureDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import loghub.Expression;

public class BeansManager {

    private final Map<Class<?>, Map<String, Method>> beans = new HashMap<>();

    public Optional<Method> getBeanByType(Object beanObject, Class<?> beanType) {
        return beans.computeIfAbsent(beanObject.getClass(), c -> {
            try {
                return Stream.of(Introspector.getBeanInfo(c, Object.class).getPropertyDescriptors())
                               .filter(pd -> pd.getWriteMethod() != null)
                               .collect(Collectors.toMap(FeatureDescriptor::getName, PropertyDescriptor::getWriteMethod));
            } catch (IntrospectionException e) {
                return Collections.emptyMap();
            }
        }).values().stream().filter(m -> m.getParameterTypes()[0].isAssignableFrom(beanType)).findFirst();
    }

    private Method beanResolver(Object beanObject, String beanName) {
        return beans.computeIfAbsent(beanObject.getClass(), c -> {
            try {
                return Stream.of(Introspector.getBeanInfo(c, Object.class).getPropertyDescriptors())
                             .filter(pd -> pd.getWriteMethod() != null)
                             .collect(Collectors.toMap(FeatureDescriptor::getName, PropertyDescriptor::getWriteMethod));
            } catch (IntrospectionException e) {
                return Collections.emptyMap();
            }
        }).get(beanName);
    }

    /**
     * Given an object, a bean name and a bean value, try to set the bean.
     * 
     * @param beanObject the object to set
     * @param beanName the bean to set
     * @param beanValue the bean value
     * @throws InvocationTargetException if unable to set bean
     */
    public void beanSetter(Object beanObject, String beanName, Object beanValue) throws InvocationTargetException, IntrospectionException {
        Method setMethod = beanResolver(beanObject, beanName);
        if (setMethod == null) {
            throw new IntrospectionException("Unknown bean '" + beanName + "' for " + beanObject.getClass().getName().replace("$Builder", ""));
        }
        if (setMethod.getParameterTypes()[0] == Expression.class) {
            // If it's not already an expression, it's a constant value that was not detected by the parser
            beanValue = beanValue instanceof Expression ? (Expression) beanValue : new Expression(beanValue);
        }
        beanSetter(beanName, beanObject, beanObject.getClass().getName().replace("$Builder", ""), setMethod, beanValue);
    }

    public void beanSetter(String beanName, Object object, String objectClassName, Method setMethod, Object beanValue) throws InvocationTargetException, IntrospectionException {
        if (setMethod == null) {
            throw new IntrospectionException("Unknown bean '" + beanName + "' for " + objectClassName);
        }
        try {
            Class<?> setArgType = setMethod.getParameterTypes()[0];
            // Array check must be the first, to ensure that a copy of the array is used, not the original argument
            if (setArgType.isArray() && beanValue.getClass().isArray()) {
                // In case of an array, try a crude conversion, expect that type cast is possible
                // for every element
                int length = Array.getLength(beanValue);
                Class<?> arrayType = setArgType.getComponentType();
                Object newValue = Array.newInstance(arrayType, length);
                for (int i = 0; i < length ; i++){
                    Array.set(newValue, i, Array.get(beanValue, i));
                }
                setMethod.invoke(object, newValue);
            } else if (beanValue == null || setArgType.isAssignableFrom(beanValue.getClass())) {
                setMethod.invoke(object, beanValue);
            } else if (beanValue instanceof String){
                Object argInstance = BeansManager.constructFromString(setArgType, (String) beanValue);
                setMethod.invoke(object, argInstance);
            } else if (beanValue instanceof Number || beanValue instanceof Character) {
                setMethod.invoke(object, beanValue);
            } else if (beanValue instanceof Boolean) {
                setMethod.invoke(object, beanValue.equals(Boolean.TRUE));
            } else {
                String message = String.format("can't assign bean %s.%s with argument type %s", objectClassName, beanName, beanValue.getClass().getName());
                throw new InvocationTargetException(new ClassCastException(message), String.format("Invalid bean %s", beanName));
            }
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw new InvocationTargetException(e, "Invalid bean '" + beanName + "' for " + objectClassName);
        }
    }

    /**
     * Create an object providing the class and a String argument. So the class must have
     * a constructor taking only a string as an argument.
     * It can manage native type and return an boxed object
     * @param <T> The value type
     * @param clazz the class of the new object
     * @param value the value as a string
     * @return a convertion from String
     * @throws InvocationTargetException if it fails to construct the value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> T constructFromString(Class<T> clazz, String value) throws InvocationTargetException {
        try {
            Constructor<T> c;
            if (clazz == Integer.TYPE || Integer.class.equals(clazz)) {
                return (T) Integer.valueOf(value);
            } else if (clazz == Double.TYPE || Double.class.equals(clazz)) {
                return (T) Double.valueOf(value);
            } else if (clazz == Float.TYPE || Float.class.equals(clazz)) {
                return (T) Float.valueOf(value);
            } else if (clazz == Byte.TYPE || Byte.class.equals(clazz)) {
                return (T) Byte.valueOf(value);
            } else if (clazz == Long.TYPE || Long.class.equals(clazz)) {
                return (T) Long.valueOf(value);
            } else if (clazz == Short.TYPE || Short.class.equals(clazz)) {
                return (T) Short.valueOf(value);
            } else if (clazz == Boolean.TYPE || Boolean.class.equals(clazz)) {
                return (T) Boolean.valueOf(value);
            } else if (clazz == Character.TYPE || Character.class.equals(clazz) && value.length() == 1) {
                return (T) Character.valueOf(value.charAt(0));
            } else if (Enum.class.isAssignableFrom(clazz)) {
                try {
                    return (T) Enum.valueOf((Class)clazz, value);
                } catch (IllegalArgumentException e) {
                    return (T) Arrays.stream(clazz.getEnumConstants())
                                     .map(Object::toString)
                                     .filter(s -> s.equalsIgnoreCase(value))
                                     .findAny()
                                     .map(s -> Enum.valueOf((Class)clazz, s))
                                     .orElseThrow(() -> new IllegalArgumentException("Not matching value " + value));
                }
            } else {
                c = clazz.getConstructor(String.class);
            }
            return c.newInstance(value);
        } catch (SecurityException | NoSuchMethodException | IllegalArgumentException |
                 InstantiationException | IllegalAccessException ex) {
            throw new InvocationTargetException(ex, clazz.getName());
        }
    }

}
