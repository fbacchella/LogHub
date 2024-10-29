package loghub.groovy;

import groovy.lang.MetaClass;
import loghub.Expression;

/**
 * This wrapper object can be called when doing comparison of a collection
 */
public class ObjectMetaClass extends LoghubMetaClass<Object> {

    public ObjectMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    protected Object callMethod(Object object, GroovyMethods method, Object argument) {
        if (GroovyMethods.EQUALS ==  method) {
            return Expression.compare("==", object, argument);
        } else if (GroovyMethods.COMPARE_TO == method) {
            return Expression.compare("<=>", object, argument);
        } else {
            return superInvokeMethod(object, method.groovyMethod, new Object[]{argument});
        }
    }

    @Override
    protected boolean isHandledClass(Object o) {
        return true;
    }

}
