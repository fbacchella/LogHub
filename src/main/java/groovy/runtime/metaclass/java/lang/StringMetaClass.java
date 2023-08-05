package groovy.runtime.metaclass.java.lang;

import org.codehaus.groovy.runtime.StringGroovyMethods;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyMethods;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class StringMetaClass extends DelegatingMetaClass {

    public StringMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods method = GroovyMethods.resolveGroovyName(methodName);
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            if (method == GroovyMethods.COMPARE_TO) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        switch (method) {
        case COMPARE_TO:
            if (arguments[0] instanceof CharSequence) {
                return object.toString().compareTo(arguments[0].toString());
            } else {
                return false;
            }
        case AS_BOOLEAN:
            return StringGroovyMethods.asBoolean(object.toString());
        case PLUS:
            return object.toString() + arguments[0].toString();
        case AS_TYPE:
            return arguments[0] == Character.TYPE ?  object.toString().charAt(0): super.invokeMethod(object, methodName, arguments);
        default:
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
