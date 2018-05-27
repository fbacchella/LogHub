package loghub.netty.servers;

public class UdpServer extends AbstractUdpServer<UdpServer, UdpServer.Builder> {

    public static class Builder extends AbstractUdpServer.Builder<UdpServer, UdpServer.Builder> {
        @Override
        public final UdpServer build() {
            return new UdpServer(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    protected UdpServer(Builder builder) {
        super(builder);
    }

}
