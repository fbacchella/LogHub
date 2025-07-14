package loghub.cloners;

import java.io.Externalizable;

public abstract class ObjectFaster<T> implements Externalizable {
    protected T value;

    protected ObjectFaster(T o) {
        value = o;
    }

    protected ObjectFaster() {
        // No value
    }

    public T get() {
        return value;
    }
}
