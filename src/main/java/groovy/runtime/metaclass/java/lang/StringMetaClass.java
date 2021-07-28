package groovy.runtime.metaclass.java.lang;

import org.codehaus.groovy.runtime.StringGroovyMethods;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class StringMetaClass extends DelegatingMetaClass {

    public StringMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public StringMetaClass(MetaClass theClass) {
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
        if ("compareTo".equals(methodName)) {
            if (arguments[0] instanceof CharSequence) {
                return object.toString().compareTo(arguments[0].toString());
            } else {
                return false;
            }
        } else if ("asBoolean".equals(methodName)){
            return StringGroovyMethods.asBoolean(object.toString());
        } else if ("plus".equals(methodName)){
            return object.toString() + arguments[0].toString();
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
