package loghub.cloners;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class CloneList {

    @SuppressWarnings("unchecked")
    static List<Object> clone(List<Object> oldList) {
        int size = oldList.size();
        Constructor<? extends List<Object>> sizeConstructor = null;
        Constructor<? extends List<Object>> listConstructor = null;
        for (Constructor<?> c: oldList.getClass().getConstructors()) {
            if (c.getParameterCount() == 1 && c.getParameterTypes()[0] == Integer.TYPE) {
                sizeConstructor = (Constructor<? extends List<Object>>) c;
                break;
            } else if (c.getParameterCount() == 1 && c.getParameterTypes()[0].isAssignableFrom(List.class)) {
                listConstructor = (Constructor<? extends List<Object>>) c;
            }
        }
        try {
            List<Object> newList = sizeConstructor != null ? sizeConstructor.newInstance(size) : new ArrayList<>(size);
            oldList.forEach(e -> newList.add(DeepCloner.clone(e)));
            if (sizeConstructor == null && listConstructor != null) {
                return listConstructor.newInstance(newList);
            } else {
                return newList;
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | RuntimeException e) {
            return CloneOpaque.clone(oldList);
        }
    }

    private CloneList() {}
}
