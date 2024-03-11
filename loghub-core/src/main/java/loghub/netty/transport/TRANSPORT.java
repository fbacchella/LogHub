package loghub.netty.transport;

import java.net.SocketAddress;

public enum TRANSPORT {
    LOCAL(true) {
        @SuppressWarnings("unchecked")
        @Override
        public LocalTransport.Builder getBuilder() {
            return LocalTransport.getBuilder();
        }
    },
    UDP(false) {
        @SuppressWarnings("unchecked")
        @Override
        public UdpTransport.Builder getBuilder() {
            return UdpTransport.getBuilder();
        }
    },
    TCP(true){
        @SuppressWarnings("unchecked")
        @Override
        public TcpTransport.Builder getBuilder() {
            return TcpTransport.getBuilder();
        }
    },
    UNIX_STREAM(true){
        @SuppressWarnings("unchecked")
        @Override
        public UnixStreamTransport.Builder getBuilder() {
            return UnixStreamTransport.getBuilder();
        }
    },
    UNIX_DGRAM(false){
        @SuppressWarnings("unchecked")
        @Override
        public UnixDgramTransport.Builder getBuilder() {
            return UnixDgramTransport.getBuilder();
        }
    },
    SCTP(true){
        @SuppressWarnings("unchecked")
        @Override
        public SctpTransport.Builder getBuilder() {
            return SctpTransport.getBuilder();
        }
    };
    private final boolean connectedServer;
    TRANSPORT(boolean connectedServer) {
        this.connectedServer = connectedServer;
    }
    public boolean isConnectedServer() {
        return connectedServer;
    }
    public abstract <B extends NettyTransport.Builder<S, M, ?, ?>, S extends SocketAddress, M> B getBuilder();
}
