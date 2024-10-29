package loghub.groovy;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;
import loghub.NullOrMissingValue;

public abstract class LoghubMetaClass<T> extends DelegatingMetaClass {

    private static final Logger logger = LogManager.getLogger();

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
        GroovyMethods method = GroovyMethods.resolveGroovyName(methodName);
        if (isHandledClass(object) && arguments.length == 1) {
            return invokeTypedMethod(object, method, arguments[0]);
        } else if (isHandledClass(object) && arguments.length == 0) {
            return invokeTypedMethod(object, method);
        } else {
            logger.info("Unhandled invoke {} {}: {}", object::getClass, () -> methodName, () -> arguments.length);
            assert false: String.format("Unhandled invoke %s %s %s", object.getClass(), methodName, Arrays.toString(arguments));
            return super.invokeMethod(object, methodName, arguments);
        }
    }

    public Object superInvokeMethod(Object object, String methodName, Object[] arguments) {
        try {
            return super.invokeMethod(object, methodName, arguments);
        } catch (RuntimeException ex) {
            // log4j2 unable to handle correctly exception with no stack
            logger.atDebug()
                  .withThrowable(ex).log(
                      "Unhandled operation {} {} {}{}",
                      object::getClass, () -> methodName, () -> Arrays.toString(arguments), () -> ex.getStackTrace().length == 0 ? "\n    " : ""
            );
            throw IgnoredEventException.INSTANCE;
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
        logger.info("Unhandled invoke {} {} {}", object::getClass, () -> method.groovyMethod, () -> argument);
        assert false: String.format("Unhandled invoke %s %s: %s", object.getClass(), method, argument.getClass());
        return super.invokeMethod(object, method.groovyMethod, new Object[] { argument });
    }

    protected Object invokeMethod(Object object, GroovyMethods method) {
        logger.info("Unhandled invoke {} {}", object::getClass, () -> method.groovyMethod);
        assert false: String.format("Unhandled invoke %s %s", object.getClass(), method);
        return super.invokeMethod(object, method.groovyMethod, new Object[] { });
    }

}
