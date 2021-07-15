package groovy.runtime.metaclass.java.time;

import groovy.lang.MetaClass;
import loghub.TimeDiff;

public class InstantMetaClass extends TimeDiff {

    public InstantMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public InstantMetaClass(MetaClass theClass) {
        super(theClass);
    }

}
