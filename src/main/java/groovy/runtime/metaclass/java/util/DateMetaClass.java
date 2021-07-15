package groovy.runtime.metaclass.java.util;

import groovy.lang.MetaClass;
import loghub.TimeDiff;

public class DateMetaClass extends TimeDiff {

    public DateMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public DateMetaClass(MetaClass theClass) {
        super(theClass);
    }

}
