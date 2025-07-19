package loghub.cloners;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;

public class CloneCollection {

    @SuppressWarnings("unchecked")
    static Collection<Object> clone(Collection<Object> oldList) {
        int size = oldList.size();
        Constructor<? extends Collection<Object>> sizeConstructor = null;
        Constructor<? extends Collection<Object>> collectionConstructor = null;
        for (Constructor<?> c: oldList.getClass().getConstructors()) {
            if (c.getParameterCount() == 1 && c.getParameterTypes()[0] == Integer.TYPE) {
                sizeConstructor = (Constructor<? extends Collection<Object>>) c;
                break;
            } else if (c.getParameterCount() == 1 && c.getParameterTypes()[0].isAssignableFrom(Collection.class)) {
                collectionConstructor = (Constructor<? extends Collection<Object>>) c;
            }
        }
        try {
            Collection<Object> newList = sizeConstructor != null ? sizeConstructor.newInstance(size) : new ArrayList<>(size);
            oldList.forEach(e -> newList.add(DeepCloner.clone(e)));
            if (sizeConstructor == null && collectionConstructor != null) {
                return collectionConstructor.newInstance(newList);
            } else {
                return newList;
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | RuntimeException e) {
            return CloneOpaque.clone(oldList);
        }
    }

    private CloneCollection() {}
}
