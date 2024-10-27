package loghub.groovy;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import groovy.lang.MetaClass;

public class CollectionMetaClass extends LoghubMetaClass<Collection<Object>> {

    public CollectionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object invokeTypedMethod(Collection<Object> collection, GroovyMethods method, Object argument) {
        if (method == GroovyMethods.PLUS) {
            try {
                Collection<Object> c;
                // Explicited test because List.of() return a class with no public constructor
                if (collection instanceof List) {
                    c = new ArrayList<>();
                } else if (collection instanceof Set) {
                    c = new LinkedHashSet<>();
                } else {
                    c = collection.getClass().getConstructor().newInstance();
                }
                c.addAll(collection);
                if ( ! (argument instanceof Collection)) {
                    c.add(argument);
                } else {
                    c.addAll(convertArgument(argument));
                }
                return c;
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                // Fails to resolve using a fast path, perhaps groovy will be smarter.
                return invokeMethod(collection, method, argument);
            }
        } else {
            return invokeMethod(collection, method, argument);
        }
    }

    @Override
    protected boolean isHandledClass(Object o) {
        return o instanceof Collection;
    }

}
