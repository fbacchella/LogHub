package loghub.netty.servers;

public class TcpServer extends AbstractTcpServer<TcpServer, TcpServer.Builder> {

    public static class Builder extends AbstractTcpServer.Builder<TcpServer, TcpServer.Builder> {
        @Override
        public final TcpServer build() throws IllegalStateException, InterruptedException {
            return new TcpServer(this);
        }
    }

    public static Builder getBuilder() {
        return new Builder();
    }

    protected TcpServer(Builder builder) throws IllegalStateException, InterruptedException {
        super(builder);
    }

}
