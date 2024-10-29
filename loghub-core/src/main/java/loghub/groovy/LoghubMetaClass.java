package loghub.groovy;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public abstract class LoghubMetaClass<T> extends DelegatingMetaClass {

    protected LoghubMetaClass(MetaClass theClass) {
        super(theClass);
    }

    protected abstract Object callMethod(T object, GroovyMethods method, Object argument);

    protected Object callMethod(T object, GroovyMethods method) {
        return invokeMethod(object, method);
    }

    protected abstract boolean isHandledClass(Object o);

    protected boolean handleNullOrMissing(GroovyMethods method) {
        if (method == GroovyMethods.COMPARE_TO) {
            return false;
        } else {
            throw IgnoredEventException.INSTANCE;
        }
    }

    @SuppressWarnings("unchecked")
    protected T convertArgument(Object object) {
        return (T) object;
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        GroovyMethods method = GroovyMethods.resolveSymbol(methodName);
        if (isHandledClass(object) && arguments.length == 1) {
            return invokeTypedMethod(object, method, arguments[0]);
        } else if (isHandledClass(object) && arguments.length == 0) {
            return invokeTypedMethod(object, method);
        } else {
            assert false: String.format("Unhandled invoke %s %s: %d", object.getClass(), methodName, arguments.length);
            return super.invokeMethod(object, methodName, arguments);
        }
    }

    public Object invokeTypedMethod(Object object, GroovyMethods method, Object argument) {
        if (argument instanceof NullOrMissingValue) {
            return handleNullOrMissing(method);
        } else {
            return callMethod(convertArgument(object), method, argument);
        }
    }

    public Object invokeTypedMethod(Object object, GroovyMethods method) {
        return callMethod(convertArgument(object), method);
    }

    protected Object invokeMethod(Object object, GroovyMethods method, Object argument) {
        assert false: String.format("Unhandled invoke %s %s: %s", object.getClass(), method, argument.getClass());
        return super.invokeMethod(object, method.groovyMethod, new Object[] { argument });
    }

    protected Object invokeMethod(Object object, GroovyMethods method) {
        assert false: String.format("Unhandled invoke %s %s: %d", object.getClass(), method);
        return super.invokeMethod(object, method.groovyMethod, new Object[] { });
    }

}
