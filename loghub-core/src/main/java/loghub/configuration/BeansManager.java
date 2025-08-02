package loghub.configuration;

import java.beans.FeatureDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import loghub.Expression;

public class BeansManager {

    private static final Logger logger = LogManager.getLogger();

    private final Map<Class<?>, Map<String, Method>> beans = new HashMap<>();

    public Method getBean(Class<?> clazz, String beanName) {
        return beans.computeIfAbsent(clazz, this::resolveBeansSetter).get(beanName);
    }

    public Optional<Method> getBeanByType(Object beanObject, Class<?> beanType) {
        return beans.computeIfAbsent(beanObject.getClass(), this::resolveBeansSetter).values().stream().filter(m -> m.getParameterTypes()[0].isAssignableFrom(beanType)).findFirst();
    }

    public Map<String, Method> resolveBeansSetter(Class<?> inspectedClass) {
        try {
            Map<String, Method> introspectedBeans = Stream.of(Introspector.getBeanInfo(inspectedClass, Object.class).getPropertyDescriptors())
                                                            .filter(pd -> pd.getWriteMethod() != null)
                                                            .collect(Collectors.toMap(FeatureDescriptor::getName, PropertyDescriptor::getWriteMethod));
            Map<String, Method> beans = new HashMap<>(introspectedBeans);
            Class<?> resolvingClass = inspectedClass;
            while (resolvingClass != null) {
                BeansPostProcess annotation = resolvingClass.getAnnotation(BeansPostProcess.class);
                if (annotation != null) {
                    Class<? extends BeansPostProcess.Processor> processorClass = annotation.value();
                    BeansPostProcess.Processor processor = processorClass.getConstructor().newInstance();
                    processor.process(beans);
                }
                resolvingClass = resolvingClass.getSuperclass();
            }
            logger.debug("Found beans for {}: {}", inspectedClass::getName, beans::keySet);
            return beans;
        } catch (IntrospectionException | InstantiationException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ex) {
            logger.atError().withThrowable(ex).log("Failed to resolve beans for {}", inspectedClass::getName);
            return Map.of();
        }
    }

    private Method beanResolver(Object beanObject, String beanName) {
        return beans.computeIfAbsent(beanObject.getClass(), this::resolveBeansSetter).get(beanName);
    }

    /**
     * Given an object, a bean name and a bean value, try to set the bean.
     *
     * @param beanObject the object to set
     * @param beanName the bean to set
     * @param beanValue the bean value
     * @throws InvocationTargetException if unable to set bean
     */
    public void beanSetter(Object beanObject, String beanName, Object beanValue)
            throws InvocationTargetException, IntrospectionException {
        beanSetter(beanObject, beanName, beanValue, beanObject.getClass().getClassLoader());
    }

    public void beanSetter(Object beanObject, String beanName, Object beanValue, ClassLoader classLoader)
            throws InvocationTargetException, IntrospectionException {
        Method setMethod = beanResolver(beanObject, beanName);
        if (setMethod == null) {
            throw new IntrospectionException("Unknown bean '" + beanName + "' for " + beanObject.getClass().getName().replace("$Builder", ""));
        }
        if (setMethod.getParameterTypes()[0] == Expression.class) {
            // If it's not already an expression, it's a constant value that was not detected by the parser
            beanValue = beanValue instanceof Expression ? (Expression) beanValue : new Expression(beanValue);
        }
        beanSetter(beanName, beanObject, beanObject.getClass().getName().replace("$Builder", ""), setMethod, beanValue, classLoader);
    }

    public void beanSetter(String beanName, Object object, String objectClassName, Method setMethod, Object beanValue, ClassLoader classLoader)
            throws InvocationTargetException, IntrospectionException {
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
                for (int i = 0; i < length; i++) {
                    Array.set(newValue, i, Array.get(beanValue, i));
                }
                setMethod.invoke(object, newValue);
            } else if (beanValue == null || setArgType.isAssignableFrom(beanValue.getClass())) {
                setMethod.invoke(object, beanValue);
            } else if (beanValue instanceof String && setMethod.getParameterTypes()[0] == Class.class) {
                try {
                    Object argInstance = classLoader.loadClass((String) beanValue);
                    setMethod.invoke(object, argInstance);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Class '" + beanValue + "' not found");
                }
            } else if (beanValue instanceof String) {
                Object argInstance = constructFromString(setArgType, (String) beanValue);
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
     * It can manage native type and return a boxed object
     * @param <T> The value type
     * @param clazz the class of the new object
     * @param value the value as a string
     * @return a conversion from String
     * @throws InvocationTargetException if it fails to construct the value
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> T constructFromString(Class<T> clazz, String value) throws InvocationTargetException {
        if (InputStream.class.isAssignableFrom(clazz)
            || OutputStream.class.isAssignableFrom(clazz)
            || Collection.class.isAssignableFrom(clazz)
            || URL.class.isAssignableFrom(clazz)
        ) {
            throw new InvocationTargetException(new IntrospectionException("Filtered class " + clazz.getName()));
        }
        try {
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
            } else if (clazz == InetAddress.class) {
                return (T) InetAddress.getByName(value);
            } else if (clazz == Character.TYPE || Character.class.equals(clazz) && value.length() == 1) {
                return (T) Character.valueOf(value.charAt(0));
            } else if (Enum.class.isAssignableFrom(clazz)) {
                return (T) resolveEnum((Class<? extends Enum>) clazz, value);
            } else if (clazz == Duration.class) {
                return (T) Duration.parse(value);
            } else {
                return (T) constructCache.computeIfAbsent(clazz, BeansManager::stringConstructor).get().create(value);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new InvocationTargetException(ex, clazz.getName());
        } catch (ExecutionException ex) {
            throw new InvocationTargetException(ex.getCause(), clazz.getName());
        } catch (RuntimeException | UnknownHostException ex) {
            throw new InvocationTargetException(ex, clazz.getName());
        }
    }

    private static <E extends Enum<E>> E resolveEnum(Class<E> clazz, String value) {
        try {
            return Enum.valueOf(clazz, value);
        } catch (IllegalArgumentException e) {
            return Arrays.stream(clazz.getEnumConstants())
                               .map(Object::toString)
                               .filter(s -> s.equalsIgnoreCase(value))
                               .findAny()
                               .map(s -> Enum.valueOf(clazz, s))
                               .orElseThrow(() -> new IllegalArgumentException("Not matching value " + value));
        }
    }

    private static final Map<Class<?>, Future<Resolve<?>>> constructCache = new ConcurrentHashMap<>();
    private static final MethodHandles.Lookup lookup = MethodHandles.publicLookup();
    private interface Resolve<T> {
        T create(String s) throws InvocationTargetException;
    }

    private static Future<Resolve<?>> stringConstructor(Class<?> clazz) {
        MethodHandle trymh;
        try {
            MethodType constructorType = MethodType.methodType(void.class, String.class);
            trymh = lookup.findConstructor(clazz, constructorType);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            List<Method> handles = new ArrayList<>();
            for (Method method : clazz.getDeclaredMethods()) {
                Class<?>[] paramTypes = method.getParameterTypes();
                if (Modifier.isStatic(method.getModifiers())
                    && method.getReturnType().equals(clazz)
                    && Modifier.isPublic(method.getModifiers())
                    && (paramTypes.length == 1)
                    && (paramTypes[0].equals(String.class) || paramTypes[0].equals(CharSequence.class))
                ) {
                    handles.add(method);
                }
            }
            if (handles.isEmpty()) {
                throw new IllegalArgumentException("Not convertible from string");
            } else {
                try {
                    trymh = lookup.unreflect(handles.getFirst());
                } catch (IllegalAccessException ex) {
                    throw new IllegalArgumentException("Not convertible from string");
                }
            }
        }
        MethodHandle mh = trymh;
        return CompletableFuture.completedFuture(s -> {
            try {
                return mh.invoke(s);
            } catch (Throwable ex) {
                throw new InvocationTargetException(ex);
            }
        });
    }

}
