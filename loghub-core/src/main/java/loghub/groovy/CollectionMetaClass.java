package loghub.groovy;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;

public class CollectionMetaClass extends DelegatingMetaClass {

    public CollectionMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods method = GroovyMethods.resolveGroovyName(methodName);
        @SuppressWarnings("unchecked")
        Collection<Object> collection = (Collection<Object>)object;
        if (method == GroovyMethods.PLUS) {
            try {
                @SuppressWarnings("unchecked")
                Collection<Object> c = collection.getClass().getConstructor().newInstance();
                c.addAll(collection);
                if ( ! (arguments[0] instanceof Collection)) {
                    c.add(arguments[0]);
                } else {
                    c.addAll((Collection)arguments[0]);
                }
                return c;
            } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
                // Fails to resolve using a fast path, perhaps groovy will be smarter.
                return super.invokeMethod(object, methodName, arguments);
            }
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
