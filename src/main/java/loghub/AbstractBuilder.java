package loghub;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractBuilder<B extends Object> {

    /**
     * To be used when the build object might need an expression builder
     */
    public interface WithCompiler {
        public void setCompiler(Function<String, Expression> compiler);
    }

    private static final Map<Class<? extends AbstractBuilder< ? extends Object>>, Map<String, Method>> cache = new HashMap<>();

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
