package loghub.groovy;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.Expression;

/**
 * This wrapper object can be called when doing comparison of a collection
 */
public class ObjectMetaClass extends DelegatingMetaClass {
    public ObjectMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        if (GroovyMethods.EQUALS.groovyMethod.equals(methodName) && arguments.length == 1) {
            return Expression.compare("==", object, arguments[0]);
        } else if (GroovyMethods.COMPARE_TO.groovyMethod.equals(methodName) && arguments.length == 1) {
            return Expression.compare("<=>", object, arguments[0]);
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
