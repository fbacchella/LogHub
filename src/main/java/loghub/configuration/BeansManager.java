package loghub.configuration;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

public class BeansManager {

    private BeansManager() {
    }

    /**
     * Given an object, a bean name and a bean value, try to set the bean.
     * 
     * The bean type is expect to have a constructor taking a String argument
     * @param beanObject the object to set
     * @param beanName the bean to set
     * @param beanValue the bean value, as a string
     * @throws InvocationTargetException if unable to set bean
     */
    static public void beanSetter(Object beanObject, String beanName, String beanValue) throws InvocationTargetException{
        try {
            PropertyDescriptor bean = new PropertyDescriptor(beanName, beanObject.getClass());
            Method setMethod = bean.getWriteMethod();
            if(setMethod == null) {
                throw new InvocationTargetException(new NullPointerException(), String.format("Unknown bean %s", beanName));
            }
            Class<?> setArgType = bean.getPropertyType();
            Object argInstance = ConstructFromString(setArgType, beanValue);
            setMethod.invoke(beanObject, argInstance);
        } catch (Exception e) {
            throw new InvocationTargetException(e, "invalid bean '" + beanName + "' for " + beanObject);
        }
    }

    /**
     * Given an object, a bean name and a bean value, try to set the bean.
     * 
     * @param beanObject the object to set
     * @param beanName the bean to set
     * @param beanValue the bean value
     * @throws InvocationTargetException if unable to set bean
     */
    static public void beanSetter(Object beanObject, String beanName, Object beanValue) throws InvocationTargetException{
        try {
            PropertyDescriptor bean = new PropertyDescriptor(beanName, beanObject.getClass());
            Method setMethod = bean.getWriteMethod();
            if(setMethod == null) {
                throw new InvocationTargetException(new NullPointerException(), String.format("Unknown bean %s", beanName));
            }
            Class<?> setArgType = bean.getPropertyType();
            if(beanValue == null || setArgType.isAssignableFrom(beanValue.getClass())) {
                setMethod.invoke(beanObject, beanValue);
            } else if (beanValue instanceof String){
                Object argInstance = BeansManager.ConstructFromString(setArgType, (String) beanValue);
                setMethod.invoke(beanObject, argInstance);
            } else if (beanValue instanceof Number){
                setMethod.invoke(beanObject, beanValue);
            } else if (beanValue instanceof Boolean){
                setMethod.invoke(beanObject, (Boolean)beanValue.equals(Boolean.TRUE));
            } else if(setArgType.isArray() && beanValue.getClass().isArray()) {
                // In case of an array, try a crude conversion, expect that type cast is possible
                // for every element
                int length = Array.getLength(beanValue);
                Class<?> arrayType = setArgType.getComponentType();
                Object newValue = Array.newInstance(arrayType, length);
                for(int i = 0; i < length ; i++){
                    Array.set(newValue, i, Array.get(beanValue, i));
                }
                setMethod.invoke(beanObject, newValue);
            } else {
                String message = String.format("can't assign bean %s.%s with argument type %s", beanObject.getClass().getName(), beanName, beanValue.getClass().getName());
                throw new InvocationTargetException(new ClassCastException(message), String.format("Invalid bean %s", beanName));
            }
        } catch (IntrospectionException e) {
            throw new InvocationTargetException(e, "Unknown bean '" + beanName + "' for " + beanObject);
        } catch (InvocationTargetException e) {
            // No need to wrap again the exception
            throw e;
        } catch (Exception e) {
            throw new InvocationTargetException(e, "invalid bean '" + beanName + "' for " + beanObject);
        }
    }

    /**
     * Enumerate the hierarchy of annotation for a class, until a certain class type is reached
     * @param searched the Class where the annotation is searched
     * @param annontationClass the annotation class
     * @param stop a class that will stop (included) the search 
     * @return a set of enumeration of type T
     */
    static public <T extends Annotation> Set<T> enumerateAnnotation(Class<?> searched, Class<T> annontationClass, Class<?> stop) {
        Set<T> annotations =  new LinkedHashSet<T>();
        while(searched != null && stop.isAssignableFrom(searched)) {
            if(searched.isAnnotationPresent(annontationClass)) {
                T annotation = searched.getAnnotation(annontationClass);
                annotations.add(annotation);
            }
            for(Class<?> i: searched.getInterfaces()) {
                if(i.isAnnotationPresent(annontationClass)) {
                    T annotation = i.getAnnotation(annontationClass);
                    annotations.add(annotation);
                }
            }
            searched = searched.getSuperclass();
        }
        return annotations;
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
    @SuppressWarnings("unchecked")
    public static <T> T ConstructFromString(Class<T> clazz, String value) throws InvocationTargetException {
        try {
            Constructor<T> c = null;
            if(clazz == Integer.TYPE || Integer.class.equals(clazz)) {
                return (T) Integer.valueOf(value);
            }
            else if(clazz == Double.TYPE || Integer.class.equals(clazz)) {
                return (T) Double.valueOf(value);
            }
            else if(clazz == Float.TYPE || Float.class.equals(clazz)) {
                return (T) Float.valueOf(value);
            }
            else if(clazz == Byte.TYPE || Byte.class.equals(clazz)) {
                return (T) Byte.valueOf(value);
            }
            else if(clazz == Long.TYPE || Long.class.equals(clazz)) {
                return (T) Long.valueOf(value);
            }
            else if(clazz == Short.TYPE || Short.class.equals(clazz)) {
                return (T) Short.valueOf(value);
            }
            else if(clazz == Boolean.TYPE || Boolean.class.equals(clazz)) {
                c = (Constructor<T>)Boolean.class.getConstructor(String.class);
            }
            else if(clazz == Character.TYPE || Character.class.equals(clazz)) {
                c = (Constructor<T>) Character.class.getConstructor(String.class);
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
