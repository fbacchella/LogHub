package loghub.cloners;

import java.lang.reflect.Array;
import java.util.Arrays;

public class CloneArray {

    static Object clone(Object o) {
        Class<?> component = o.getClass().getComponentType();
        int length = Array.getLength(o);
        if (component.isPrimitive()) {
            if (component == Boolean.TYPE) {
                return Arrays.copyOf((boolean[]) o, length);
            } else if (component == Byte.TYPE) {
                return Arrays.copyOf((byte[]) o, length);
            } else if (component == Short.TYPE) {
                return Arrays.copyOf((short[]) o, length);
            } else if (component == Integer.TYPE) {
                return Arrays.copyOf((int[]) o, length);
            } else if (component == Long.TYPE) {
                return Arrays.copyOf((long[]) o, length);
            } else if (component == Float.TYPE) {
                return Arrays.copyOf((float[]) o, length);
            } else if (component == Double.TYPE) {
                return Arrays.copyOf((double[]) o, length);
            } else if (component == Character.TYPE) {
                return Arrays.copyOf((char[]) o, length);
            } else {
                throw new IllegalStateException("Unreachable code");
            }
        } else if (DeepCloner.isImmutable(component)) {
            return Arrays.copyOf((Object[]) o, length);
        } else {
            Object array = Array.newInstance(component, length);
            for (int i = 0; i < length; i++) {
                Array.set(array, i, DeepCloner.clone(Array.get(o, i)));
            }
            return array;
        }
    }

    private CloneArray() {}

}
