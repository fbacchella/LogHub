package loghub;

import java.lang.reflect.InvocationTargetException;

public abstract class AbstractBuilder<B> {

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
