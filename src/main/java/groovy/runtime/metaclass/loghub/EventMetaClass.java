package groovy.runtime.metaclass.loghub;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.events.Event;

public class EventMetaClass extends DelegatingMetaClass {

    public EventMetaClass(Class<?> theClass) {
        super(theClass);
    }

    public EventMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        Event ev = (Event) object;
        switch(methodName) {
        case "getTimestamp": return ev.getTimestamp();
        case "getConnectionContext": return ev.getConnectionContext();
        case "getMeta": return ev.getMeta(arguments[0].toString());
        case "getGroovyPath": return ev.getGroovyPath(arguments);
        case "getGroovyIndirectPath": return ev.getGroovyIndirectPath(arguments);
        default: {
            assert false;
            return super.invokeMethod(object, methodName, arguments);
        }
        }
    }

}
