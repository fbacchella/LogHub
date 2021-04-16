package groovy.runtime.metaclass.loghub;

import groovy.lang.MetaClass;

public class EventInstanceMetaClass extends EventMetaClass {

    public EventInstanceMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public EventInstanceMetaClass(MetaClass theClass) {
        super(theClass);
    }

}
