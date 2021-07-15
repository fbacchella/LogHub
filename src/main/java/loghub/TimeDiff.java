package loghub;

import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.Date;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;

public abstract class TimeDiff extends DelegatingMetaClass {

    public TimeDiff(Class<?> theClass) {
        super(theClass);
    }

    public TimeDiff(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        if (! "minus".equals(methodName) || arguments.length != 1) {
            return super.invokeMethod(object, methodName, arguments);
        }
        Temporal arg1 = resolveTime(object);
        Temporal arg2 = resolveTime(arguments[0]);
        if (arg1 == null || arg2 == null) {
            return super.invokeMethod(object, methodName, arguments);
        }
        Duration d = Duration.between(arg1, arg2);
        return (double) d.getSeconds() + (double)d.getNano()*1e-9;
    }

    private Temporal resolveTime(Object object) {
        if (object instanceof Temporal) {
            return (Temporal) object;
        } else if (object instanceof Date) {
            return ((Date) object).toInstant();
        } else {
            return null;
        }
    }

}
