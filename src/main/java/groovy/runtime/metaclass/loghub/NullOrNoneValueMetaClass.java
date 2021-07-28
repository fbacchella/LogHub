package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NullOrNoneValueMetaClass extends DelegatingMetaClass {

    public NullOrNoneValueMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public NullOrNoneValueMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        NullOrMissingValue val = (NullOrMissingValue) object;
        switch (methodName) {
        case "equals": return val.equals(arguments[0]);
        case "compareTo": return val.compareTo(arguments[0]);
        case "isCase": return false;
        case "asBoolean": return false;
        default:
            throw IgnoredEventException.INSTANCE;
        }
    }

}
