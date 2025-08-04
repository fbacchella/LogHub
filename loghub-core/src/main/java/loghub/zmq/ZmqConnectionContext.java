package loghub.zmq;

import java.io.Serializable;
import java.security.Principal;

import loghub.BuildableConnectionContext;
import loghub.cloners.Immutable;
import lombok.Data;
import zmq.Msg;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;

@Immutable
public class ZmqConnectionContext extends BuildableConnectionContext<String> {

    private final String selfAddress;
    private final String peerAddress;

    @Data
    public static class ZmqPrincipal implements Principal, Serializable {
        private final String name;
        private final Mechanisms mechanism;
    }

    public ZmqConnectionContext(Msg msg, Mechanisms security) {
        Metadata md = msg.getMetadata();
        if (md != null) {
            peerAddress = md.get(Metadata.PEER_ADDRESS);
            selfAddress = md.get("X-Self-Address");
            if (security == Mechanisms.PLAIN || security == Mechanisms.CURVE) {
                String userId = md.get(Metadata.USER_ID);
                if (userId != null && ! userId.isBlank()) {
                    this.principal = new ZmqPrincipal(userId, security);
                }
            }
        } else {
            peerAddress = null;
            selfAddress = null;
        }
    }

    @Override
    public void setPrincipal(Principal peerPrincipal) {
        throw new IllegalStateException("Cannot rewrite principal");
    }

    @Override
    public String getLocalAddress() {
        return selfAddress;
    }

    @Override
    public String getRemoteAddress() {
        return peerAddress;
    }
}
