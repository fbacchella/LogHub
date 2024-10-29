package loghub.groovy;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.temporal.Temporal;
import java.util.Date;

import groovy.lang.MetaClass;
import loghub.IgnoredEventException;

public class TemporalMetaClass extends LoghubMetaClass<Temporal> {

    public TemporalMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    protected Object callMethod(Temporal arg1, GroovyMethods method, Object argument) {
        Temporal arg2 = convertArgument(argument);
        if ((GroovyMethods.MINUS == method || GroovyMethods.COMPARE_TO == method) && arg1 != null && arg2 != null) {
            Duration d;
            try {
                d = Duration.between(arg2, arg1);
            } catch (DateTimeException e) {
                throw IgnoredEventException.INSTANCE;
            }
            if (GroovyMethods.MINUS == method) {
                return d.getSeconds() + d.getNano() * 1e-9;
            } else {
                return d.isNegative() ? -1 : (d.isZero() ? 0 : 1);
            }
        } else if (GroovyMethods.MINUS == method) {
            throw IgnoredEventException.INSTANCE;
        } else if (GroovyMethods.COMPARE_TO == method){
            return false;
        } else {
            return invokeMethod(arg1, method, argument);
        }
    }

    @Override
    protected boolean isHandledClass(Object o) {
        return o instanceof Temporal || o instanceof Date;
    }

    @Override
    protected Temporal convertArgument(Object object) {
        if (object instanceof Temporal) {
            return (Temporal) object;
        } else if (object instanceof Date) {
            return ((Date) object).toInstant();
        } else {
            return null;
        }
    }

}
