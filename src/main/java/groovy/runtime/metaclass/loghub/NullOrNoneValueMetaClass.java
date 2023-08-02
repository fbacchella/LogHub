package groovy.runtime.metaclass.loghub;

import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyOperators;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NullOrNoneValueMetaClass extends DelegatingMetaClass {

    public NullOrNoneValueMetaClass(Class<?> theClass) {
        super(theClass);
    }

    @SuppressWarnings("unused")
    public NullOrNoneValueMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        NullOrMissingValue val = (NullOrMissingValue) object;
        switch (methodName) {
        case GroovyOperators.EQUALS: return val.equals(arguments[0]);
        case GroovyOperators.COMPARE_TO: return val.compareTo(arguments[0]);
        case "asBoolean": return false;
        default:
            throw IgnoredEventException.INSTANCE;
        }
    }

}
