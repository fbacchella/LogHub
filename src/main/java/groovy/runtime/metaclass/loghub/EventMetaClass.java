package groovy.runtime.metaclass.loghub;

import groovy.lang.MetaClass;
import loghub.events.Event;

public class EventMetaClass extends DelegatingMetaClass {

    public EventMetaClass(Class<?> theClass) {
        super(theClass);
    }

    @SuppressWarnings("unused")
    public EventMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        Event ev = (Event) object;
        switch (methodName) {
        case "getTimestamp": return ev.getTimestamp();
        case "getConnectionContext": return ev.getConnectionContext();
        case "getMeta": return ev.getMeta(arguments[0].toString());
        case "getGroovyPath": return ev.getGroovyPath((int)arguments[0]);
        case "getGroovyLastException": return ev.getGroovyLastException();
        default: {
            assert false : "Unknown method " + methodName;
            return super.invokeMethod(object, methodName, arguments);
        }
        }
    }

}
