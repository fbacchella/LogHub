package loghub.groovy;

import groovy.lang.MetaClass;

public class BooleanMetaClass extends LoghubMetaClass<Boolean> {

    public BooleanMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    protected Object callMethod(Boolean arg1, GroovyMethods method, Object argument) {
        boolean arg2 = convertArgument(argument);
        boolean value;
        switch (method) {
        case PLUS:
        case OR:
            value = arg1 || arg2;
            break;
        case AND:
        case MULTIPLY:
            value = arg1 && arg2;
            break;
        case XOR:
            value = arg1 ^ arg2;
            break;
        default:
            return invokeMethod(arg1, method, argument);
        }
        return value;
    }

    @Override
    protected Boolean convertArgument(Object object) {
        if (object instanceof CharSequence) {
            return Boolean.parseBoolean(toString());
        } else {
            return Boolean.TRUE.equals(object);
        }
    }

    @Override
    protected boolean isHandledClass(Object o) {
        return o instanceof Boolean;
    }

}
