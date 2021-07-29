package groovy.runtime.metaclass.loghub;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.Date;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.IgnoredEventException;

public class TimeDiff extends DelegatingMetaClass {

    public TimeDiff(Class<?> theClass) {
        super(theClass);
    }

    public TimeDiff(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        Temporal arg1 = resolveTime(object);
        Temporal arg2 = arguments.length == 1 ? resolveTime(arguments[0]) : null ;
        if (("minus".equals(methodName) || "compareTo".equals(methodName)) && arg1 != null && arg2 != null) {
            Duration d;
            try {
                d = Duration.between(arg2, arg1);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
            if ("minus".equals(methodName)) {
                return d.getSeconds() + d.getNano()*1e-9;
            } else {
                return d.isNegative() ? -1 : (d.isZero() ? 0 : 1);
            }
        } else if ("minus".equals(methodName)) {
            throw IgnoredEventException.INSTANCE;
        } else if ("compareTo".equals(methodName)){
            return false;
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
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
