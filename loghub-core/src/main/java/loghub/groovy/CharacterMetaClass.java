package loghub.groovy;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
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
            if (GroovyMethods.COMPARE_TO.groovyMethod.equals(methodName)) {
                return false;
            } else {
                throw IgnoredEventException.INSTANCE;
            }
        }
        if (GroovyMethods.COMPARE_TO.groovyMethod.equals(methodName)) {
            if (arguments[0] instanceof CharSequence) {
                return object.toString().compareTo((arguments[0].toString()));
            } else {
                return false;
            }
        } else if (GroovyMethods.AS_BOOLEAN.groovyMethod.equals(methodName)){
            return c != '\0';
        } else if (GroovyMethods.PLUS.groovyMethod.equals(methodName)){
            return object.toString() + arguments[0].toString();
        } else if (GroovyMethods.AS_TYPE.groovyMethod.equals(methodName) && arguments[0] == Character.TYPE){
            return c;
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

}
