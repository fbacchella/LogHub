package loghub.groovy;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public abstract class LoghubMetaClass<T> extends DelegatingMetaClass {

    protected LoghubMetaClass(MetaClass theClass) {
        super(theClass);
    }

    protected abstract Object invokeTypedMethod(T object, GroovyMethods method, Object argument);

    protected Object invokeTypedMethod(T object, GroovyMethods method) {
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
        if (arguments.length == 1 && arguments[0] instanceof NullOrMissingValue) {
            return handleNullOrMissing(method);
        } else if (isHandledClass(object) && arguments.length == 1) {
            return invokeTypedMethod(convertArgument(object), method, arguments[0]);
        } else if (isHandledClass(object) && arguments.length == 0) {
            return invokeTypedMethod(convertArgument(object), method);
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

    protected Object invokeMethod(Object object, GroovyMethods method, Object argument) {
        return super.invokeMethod(object, method.groovyMethod, new Object[] { argument });
    }

    protected Object invokeMethod(Object object, GroovyMethods method) {
        return super.invokeMethod(object, method.groovyMethod, new Object[] { });
    }

}
