package groovy.runtime.metaclass.java.lang;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import groovy.runtime.metaclass.GroovyOperators;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public class CharacterMetaClass extends DelegatingMetaClass {

    public CharacterMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        Character c = (Character) object;
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            if (GroovyOperators.COMPARE_TO.equals(methodName)) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        if (GroovyOperators.COMPARE_TO.equals(methodName)) {
            if (arguments[0] instanceof CharSequence) {
                return object.toString().compareTo((arguments[0].toString()));
            } else {
                return false;
            }
        } else if (GroovyOperators.AS_BOOLEAN.equals(methodName)){
            return c.charValue() != '\0';
        } else if (GroovyOperators.PLUS.equals(methodName)){
            return object.toString() + arguments[0].toString();
        } else if (GroovyOperators.AS_TYPE.equals(methodName) && arguments[0] == Character.TYPE){
            return c.charValue();
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
