package groovy.runtime.metaclass.loghub;

import groovy.lang.MetaClass;

public class EventWrapperMetaClass extends EventMetaClass {

    public EventWrapperMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public EventWrapperMetaClass(MetaClass theClass) {
        super(theClass);
    }

}
