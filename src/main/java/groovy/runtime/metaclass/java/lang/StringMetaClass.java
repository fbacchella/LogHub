package groovy.runtime.metaclass.java.lang;

import org.codehaus.groovy.runtime.StringGroovyMethods;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyOperators;
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
            if (GroovyOperators.COMPARE_TO.equals(methodName)) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        if (GroovyOperators.COMPARE_TO.equals(methodName)) {
            if (arguments[0] instanceof CharSequence) {
                return object.toString().compareTo(arguments[0].toString());
            } else {
                return false;
            }
        } else if (GroovyOperators.AS_BOOLEAN.equals(methodName)){
            return StringGroovyMethods.asBoolean(object.toString());
        } else if (GroovyOperators.PLUS.equals(methodName)){
            return object.toString() + arguments[0].toString();
        } else if (GroovyOperators.AS_TYPE.equals(methodName) && arguments[0] == Character.TYPE){
            return object.toString().charAt(0);
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
