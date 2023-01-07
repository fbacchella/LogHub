package loghub.zmq;

import loghub.ConnectionContext;
import zmq.Msg;
import zmq.io.Metadata;
import zmq.io.mechanism.Mechanisms;

public class ZmqConnectionContext extends ConnectionContext<String> {

    private final String selfAddress;
    private final String peerAddress;

    public ZmqConnectionContext(Msg msg, Mechanisms security) {
        Metadata md = msg.getMetadata();
        peerAddress = md.get(Metadata.PEER_ADDRESS);
        selfAddress = md.get("X-Self-Address");
        if (security == Mechanisms.PLAIN || security == Mechanisms.CURVE) {
            String userId = md.get(Metadata.USER_ID);
            setPrincipal(() -> userId);
        }
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
