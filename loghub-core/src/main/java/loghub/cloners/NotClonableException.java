package loghub.cloners;

public class NotClonableException extends Exception {

    NotClonableException(Class<?> clazz, Throwable cause) {
        super(resolveName(clazz), cause);
    }

    private static String resolveName(Class<?> clazz) {
        String name = clazz.getCanonicalName();
        return name != null ? name : clazz.getName();
    }

}
