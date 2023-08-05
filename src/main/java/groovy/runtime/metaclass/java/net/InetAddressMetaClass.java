package groovy.runtime.metaclass.java.net;

import java.net.InetAddress;
import java.net.UnknownHostException;

import groovy.lang.DelegatingMetaClass;
import groovy.lang.MetaClass;
import loghub.Helpers;

public class InetAddressMetaClass extends DelegatingMetaClass {

    public InetAddressMetaClass(MetaClass theClass) {
        super(theClass);
    }

    @Override
    public Object invokeMethod(Object object, String methodName, Object[] arguments) {
        InetAddress arg1 = resolveAddr(object);
        InetAddress arg2 = arguments.length == 1 ? resolveAddr(arguments[0]) : null ;
        if ("equals".equals(methodName) && arg1 != null && arg2 != null) {
            return arg1.equals(arg2);
        } else if ("equals".equals(methodName)) {
            return false;
        } else {
            return super.invokeMethod(object, methodName, arguments);
        }
    }

    private InetAddress resolveAddr(Object object) {
        if (object instanceof InetAddress) {
            return (InetAddress) object;
        } else {
            try {
                return Helpers.parseIpAddress(object.toString());
            } catch (UnknownHostException ex) {
                throw new IllegalArgumentException("Unknown host" + object, ex);
            }
        }
    }

}
