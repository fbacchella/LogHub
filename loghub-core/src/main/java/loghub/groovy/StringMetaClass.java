package loghub.groovy;

import groovy.lang.MetaClass;

public class StringMetaClass extends LoghubMetaClass<CharSequence> {

    public StringMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    protected Object callMethod(CharSequence object, GroovyMethods method, Object argument) {
        if (method == GroovyMethods.PLUS) {
            return object.toString() + argument.toString();
        } else {
            return invokeMethod(object, method, argument);
        }
    }

    @Override
    protected  boolean isHandledClass(Object o) {
        return o instanceof CharSequence;
    }

}
