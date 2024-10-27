package loghub.groovy;

import groovy.lang.MetaClass;

public class CharacterMetaClass extends LoghubMetaClass<Character> {

    public CharacterMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object invokeTypedMethod(Character object, GroovyMethods method, Object argument) {
        if (GroovyMethods.COMPARE_TO == method) {
            if (argument instanceof CharSequence) {
                return object.toString().compareTo((argument.toString()));
            } else {
                return false;
            }
        } else if (GroovyMethods.PLUS == method){
            return object.toString() + argument.toString();
        } else {
            return invokeMethod(object, method, argument);
        }
    }

    @Override
    public boolean isHandledClass(Object o) {
        return o instanceof Character;
    }

}
