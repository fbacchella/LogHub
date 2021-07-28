package groovy.runtime.metaclass.java.lang;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NumberMetaClass extends DelegatingMetaClass {

    public NumberMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public NumberMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            if ("compareTo".equals(methodName)) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        return super.invokeMethod(object, methodName, arguments);
    }

}
