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
        Temporal arg1 = null;
        Temporal arg2 = null;
        if ((arg2 = resolveTime(arguments[0])) == null || (arg1 = resolveTime(object)) == null) {
            return super.invokeMethod(object, methodName, arguments);
        }
        Duration d = Duration.between(arg2, arg1);
        return d.getSeconds() + d.getNano()*1e-9;
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
