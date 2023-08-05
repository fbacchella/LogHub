package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyMethods;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class NullOrNoneValueMetaClass extends DelegatingMetaClass {

    public NullOrNoneValueMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        NullOrMissingValue val = (NullOrMissingValue) object;
        switch (GroovyMethods.resolveGroovyName(methodName)) {
        case EQUALS: return val.equals(arguments[0]);
        case COMPARE_TO: return val.compareTo(arguments[0]);
        case AS_BOOLEAN: return false;
        default:
            if (object == NullOrMissingValue.NULL) {
                return NullOrMissingValue.NULL;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
    }

}
