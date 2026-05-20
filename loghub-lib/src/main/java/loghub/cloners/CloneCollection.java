package loghub.cloners;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class CloneCollection {

    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("unchecked")
    static Collection<Object> clone(Collection<Object> oldList) throws NotClonableException {
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
            for (Object e: oldList) {
                newList.add(DeepCloner.clone(e));
            }
            if (sizeConstructor == null && collectionConstructor != null) {
                return collectionConstructor.newInstance(newList);
            } else {
                return newList;
            }
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | RuntimeException e) {
            logger.atWarn().withThrowable(e).log("Failback to byte serialization clone for {}", () -> oldList.getClass().getName());
            return CloneOpaque.clone(oldList);
        }
    }

    private CloneCollection() {}
}
