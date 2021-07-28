package loghub;

import java.util.Objects;


public abstract class NullOrMissingValue implements Comparable<Object> {

    public static final NullOrMissingValue MISSING = new NullOrMissingValue() {
        @Override
        public String toString() {
            return Objects.toString("NoValue");
        }
    };

    public static final NullOrMissingValue NULL = new NullOrMissingValue() {
        @Override
        public String toString() {
            return Objects.toString("NullValue");
        }
    };

    private NullOrMissingValue() {
    }

    @Override
    public int hashCode() {
        return Objects.hash((Object)null);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == null || obj instanceof NullOrMissingValue;
    }

    @Override
    public int compareTo(Object obj) {
        if (obj == null || obj instanceof NullOrMissingValue) {
            return 0;
        } else {
            return -1;
        }
    }

}
