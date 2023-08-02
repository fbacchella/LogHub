package groovy.runtime.metaclass.loghub;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.util.Map;

import groovy.lang.MetaClass;
import loghub.ConnectionContext;
import loghub.NullOrMissingValue;

public class FastPathMetaClass  extends DelegatingMetaClass {

    public FastPathMetaClass(MetaClass delegate) {
        super(delegate);
    }

    @Override
    public Object getProperty(Object object, String property) {
        if (object == NullOrMissingValue.class && "NULL".equals(property)) {
            return NullOrMissingValue.NULL;
        } else if (object == NullOrMissingValue.class && "MISSING".equals(property)) {
            return NullOrMissingValue.MISSING;
        } else if (object instanceof ConnectionContext && "principal".equals(property)) {
            return  ((ConnectionContext)object).getPrincipal();
        } else if (object instanceof Principal && "name".equals(property)) {
            return  ((Principal)object).getName();
        } else if (object instanceof ConnectionContext && "localAddress".equals(property)) {
            return  ((ConnectionContext)object).getLocalAddress();
        } else if (object instanceof ConnectionContext && "remoteAddress".equals(property)) {
            return  ((ConnectionContext)object).getRemoteAddress();
        } else if (object instanceof InetAddress && "hostAddress".equals(property)) {
            return  ((InetAddress)object).getHostAddress();
        } else if (object instanceof InetSocketAddress && "address".equals(property)) {
            return  ((InetSocketAddress)object).getAddress();
        } else if (object instanceof Map) {
            return ((Map)object).get(property);
        } else {
            assert false : String.format("getProperty %s[%s].(%s)%n", object.getClass(), property);
            return super.getProperty(object, property);
        }
    }

}
