package loghub;

import java.util.Objects;


public abstract class NullOrMissingValue {

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

    public boolean compareTo(Object obj) {
        if (obj == null || obj instanceof NullOrMissingValue) {
            return true;
        } else {
            return false;
        }
    }

}
