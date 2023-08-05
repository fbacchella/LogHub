package groovy.runtime.metaclass.java.lang;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class BooleanMetaClass extends DelegatingMetaClass {

    public BooleanMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        @SuppressWarnings("unchecked")
        Boolean bool = (Boolean) object;
        for (Object argument : arguments) {
            if (argument instanceof NullOrMissingValue) {
                throw IgnoredEventException.INSTANCE;
            }
        }
        if ("compareTo".equals(methodName)) {
            if (! (arguments[0] instanceof Boolean)) {
                throw new ClassCastException(arguments[0] + " not a boolean");
            } else {
                return bool.compareTo((Boolean) arguments[0]);
            }
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
