package loghub.netty.servers;

public class UdpServer extends AbstractUdpServer<UdpServer, UdpServer.Builder> {

    public static class Builder extends AbstractUdpServer.Builder<UdpServer, UdpServer.Builder> {
        @Override
        public final UdpServer build() throws IllegalStateException, InterruptedException {
            return new UdpServer(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    protected UdpServer(Builder builder) throws IllegalStateException, InterruptedException {
        super(builder);
    }

}
