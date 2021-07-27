package loghub;

import java.util.Objects;

public class NoValue<T> implements Comparable<T> {

    public static final NoValue<Object> INSTANCE = new NoValue<>();

    private NoValue() {
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object)null);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == null || obj == this;
    }

    @Override
    public String toString() {
        return Objects.toString("NullValue");
    }

    @Override
    public int compareTo(Object o) {
        throw IgnoredEventException.INSTANCE;
    }

}
