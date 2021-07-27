package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NoValue;

public class NoValueMetaClass extends DelegatingMetaClass {

    public NoValueMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public NoValueMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        switch (methodName) {
        case "equals": return NoValue.INSTANCE.equals(arguments[0]);
        case "compareTo": return NoValue.INSTANCE.equals(arguments[0]);
        case "isCase": return false;
        case "asBoolean": return false;
        default:
            throw IgnoredEventException.INSTANCE;
        }
    }
    
    
}
