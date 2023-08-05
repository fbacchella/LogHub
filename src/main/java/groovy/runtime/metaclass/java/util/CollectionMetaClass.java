package groovy.runtime.metaclass.java.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyMethods;
import loghub.Helpers;

public class CollectionMetaClass extends DelegatingMetaClass {

    public CollectionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods method = GroovyMethods.resolveGroovyName(methodName);
        @SuppressWarnings("unchecked")
        Collection collection = (Collection)object;
        if (method == GroovyMethods.PLUS) {
            try {
                Collection c = collection.getClass().getConstructor().newInstance();
                c.addAll(collection);
                for (Object o: arguments) {
                    c.add(o);
                }
                return c;
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                throw new UnsupportedOperationException("Unable to duplication collection " + Helpers.resolveThrowableException(ex), ex);
            }
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
