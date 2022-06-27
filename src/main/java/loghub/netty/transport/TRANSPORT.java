package loghub.netty.transport;

import java.net.SocketAddress;

public enum TRANSPORT {
    LOCAL(true) {
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new LocalTransport(poller);
        }
    },
    UDP(false) {
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new UdpTransport(poller);
        }
    },
    TCP(true){
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new TcpTransport(poller);
        }
    },
    UNIX_STREAM(true){
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new UnixStreamTransport(poller);
        }
    },
    UNIX_DGRAM(false){
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new UnixDgramTransport(poller);
        }
    },
    SCTP(true){
        @Override
        public <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller) {
            return (T) new SctpTransport(poller);
        }
    };
    private final boolean connectedServer;
    TRANSPORT(boolean connectedServer) {
        this.connectedServer = connectedServer;
    }
    boolean isConnectedServer() {
        return connectedServer;
    }
    public abstract <T extends NettyTransport<S, M>, S extends SocketAddress, M> T getInstance(POLLER poller);
}
