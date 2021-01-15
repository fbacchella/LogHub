package loghub.configuration;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

public class BeansManager {

    private BeansManager() {
    }

    /**
     * Given an object, a bean name and a bean value, try to set the bean.
     * 
     * @param beanObject the object to set
     * @param beanName the bean to set
     * @param beanValue the bean value
     * @throws InvocationTargetException if unable to set bean
     */
    static public void beanSetter(Object beanObject, String beanName, Object beanValue) throws InvocationTargetException, IntrospectionException {
        Method setMethod;
        try {
            Optional<PropertyDescriptor> beanopt =  Optional.ofNullable(new PropertyDescriptor(beanName, beanObject.getClass()));
            setMethod = beanopt.map(PropertyDescriptor::getWriteMethod).orElse(null);
        } catch (IntrospectionException e) {
            // new PropertyDescriptor throws a useless message, will delegate it.
            setMethod = null;
        }
        beanSetter(beanName, beanObject, beanObject.getClass().getName(), setMethod, beanValue);
    }

    static public void beanSetter(String beanName, Object object, String objectClassName, Method setMethod, Object beanValue) throws InvocationTargetException, IntrospectionException {
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
                Object argInstance = BeansManager.ConstructFromString(setArgType, (String) beanValue);
                setMethod.invoke(object, argInstance);
            } else if (beanValue instanceof Number || beanValue instanceof Character) {
                setMethod.invoke(object, beanValue);
            } else if (beanValue instanceof Boolean) {
                setMethod.invoke(object, (Boolean)beanValue.equals(Boolean.TRUE));
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
     * 
     * It can manage native type and return an boxed object
     * @param <T> The value type
     * @param clazz the class of the new object
     * @param value the value as a string
     * @return a convertion from String
     * @throws InvocationTargetException if it fails to construct the value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> T ConstructFromString(Class<T> clazz, String value) throws InvocationTargetException {
        try {
            Constructor<T> c = null;
            if (clazz == Integer.TYPE || Integer.class.equals(clazz)) {
                return (T) Integer.valueOf(value);
            } else if (clazz == Double.TYPE || Integer.class.equals(clazz)) {
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
                c = (Constructor<T>)Boolean.class.getConstructor(String.class);
            } else if (clazz == Character.TYPE || Character.class.equals(clazz) && value.length() == 1) {
                return (T) Character.valueOf(value.charAt(0));
            } else if (Enum.class.isAssignableFrom(clazz)) {
                return (T) Enum.valueOf((Class)clazz, value);
            } else {
                c = clazz.getConstructor(String.class);
            }
            return c.newInstance(value);
        } catch (SecurityException e) {
            throw new InvocationTargetException(e, clazz.getName());
        } catch (NoSuchMethodException e) {
            throw new InvocationTargetException(e, clazz.getName());
        } catch (IllegalArgumentException e) {
            throw new InvocationTargetException(e, clazz.getName());
        } catch (InstantiationException e) {
            throw new InvocationTargetException(e, clazz.getName());
        } catch (IllegalAccessException e) {
            throw new InvocationTargetException(e, clazz.getName());
        } catch (InvocationTargetException e) {
            throw e;
        }
    }

}
