package loghub;

import java.io.Serializable;
import java.security.Principal;

public abstract class ConnectionContext<A> implements Serializable {
    
    private static class EmptyPrincipal implements Principal, Serializable {
        @Override
        public String getName() {
            return "";
        }
    };

    private static Principal EMPTYPRINCIPAL = new EmptyPrincipal();

    public static final ConnectionContext<Object> EMPTY = new ConnectionContext<Object>() {

        @Override
        public Object getLocalAddress() {
            return null;
        }

        @Override
        public Object getRemoteAddress() {
            return null;
        }
    };

    private Principal peerPrincipal;

    public ConnectionContext() {
        peerPrincipal = EMPTYPRINCIPAL;
    }

    public void acknowledge() {
    }

    public Principal getPrincipal() {
        return peerPrincipal;
    }

    public void setPrincipal(Principal peerPrincipal) {
        this.peerPrincipal = peerPrincipal;
    }

    public abstract A getLocalAddress();

    public abstract A getRemoteAddress();

}
