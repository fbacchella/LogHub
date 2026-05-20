package loghub.cloners;

@FunctionalInterface
public interface Cloner<T> {
    T clone(T object) throws NotClonableException;
}
