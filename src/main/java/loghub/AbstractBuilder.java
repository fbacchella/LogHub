package loghub;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import loghub.configuration.BeansManager;

public abstract class AbstractBuilder<B extends Object> {

    /**
     * To be used when the build object might need an expression builder
     */
    public interface WithCompiler {
        public void setCompiler(Function<String, Expression> compiler);
    }

    private static final Map<Class<? extends AbstractBuilder< ? extends Object>>, Map<String, Method>> cache = new HashMap<>();

    private Method resolve(String beanName) {
        if (!cache.containsKey(getClass())) {
            @SuppressWarnings("unchecked")
            Class<? extends AbstractBuilder< ? extends Object>> clazz = (Class<? extends AbstractBuilder<? extends Object>>) getClass();
            // Custom resolution of setters, because Lombock setters are not Java beans setters
            Map<String, Method> beans = Arrays.stream(clazz.getMethods())
                            .filter(i -> i.getName().startsWith("set"))
                            .filter(i -> i.getParameterTypes().length == 1)
                            .collect(Collectors.toMap(i -> Introspector.decapitalize( i.getName().substring(3)), i -> i));
            cache.put(clazz, beans);
        }
        return cache.get(getClass()).get(beanName);
    }

    public void set(String beanName, Object beanValue) throws InvocationTargetException, IntrospectionException {
        Method setMethod = resolve(beanName);
        String objectClassName = getClass().getEnclosingClass().getCanonicalName();
        if(setMethod == null) {
            throw new IntrospectionException(String.format("Unknown bean '%s' for %s", beanName, objectClassName));
        }
        BeansManager.beanSetter(beanName, this, objectClassName, setMethod, beanValue);
    }

    public abstract B build();

    public static <B extends Object> AbstractBuilder<B> resolve(Class<B> objectClass) throws InvocationTargetException{
        BuilderClass bca = objectClass.getAnnotation(BuilderClass.class);
        if (bca != null) {
            try {
                @SuppressWarnings("unchecked")
                Class<? extends AbstractBuilder<B>> bc = (Class<? extends AbstractBuilder<B>>) bca.value();
                return bc.getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException
                            | IllegalArgumentException | NoSuchMethodException
                            | SecurityException | InvocationTargetException e) {
                throw new InvocationTargetException(e, "The builder is unusable: " + Helpers.resolveThrowableException(e));
            }
        } else {
            return null;
        }
    }

}
