package loghub.groovy;

import groovy.lang.MetaClass;

public class StringMetaClass extends LoghubMetaClass<CharSequence> {

    public StringMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    protected Object callMethod(CharSequence object, GroovyMethods method, Object argument) {
        switch (method) {
        case COMPARE_TO:
            if (argument instanceof CharSequence) {
                return object.toString().compareTo(argument.toString());
            } else {
                return false;
            }
        case PLUS:
            return object.toString() + argument.toString();
        default:
            return invokeMethod(object, method, argument);
        }
    }

    @Override
    protected  boolean isHandledClass(Object o) {
        return o instanceof CharSequence;
    }

}
