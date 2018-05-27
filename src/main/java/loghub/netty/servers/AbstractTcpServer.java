package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import loghub.netty.TcpFactory;

public class AbstractTcpServer<
    S extends AbstractTcpServer<S, B>,
    B extends AbstractTcpServer.Builder<S, B>
    > extends NettyIpServer<TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, S, B> {
    
    public abstract static class Builder<S extends AbstractTcpServer<S, B>,
                                         B extends AbstractTcpServer.Builder<S, B>
                                        > extends NettyIpServer.Builder<S, B> {
        protected Builder() {
        }
    }

    protected AbstractTcpServer(B builder) {
        super(builder);
    }

    Channel cf;

    @Override
    protected boolean makeChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> bootstrap, InetSocketAddress address) {
        // Bind and start to accept incoming connections.
        try {
            cf = bootstrap.bind(address).await().channel();
            logger.debug("bond to {}", address);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void configureBootStrap(ServerBootstrap bootstrap) {
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        super.configureBootStrap(bootstrap);
    }

    @Override
    protected TcpFactory getNewFactory() {
        return new TcpFactory(poller);
    }

    @Override
    public void waitClose() throws InterruptedException {
        cf.closeFuture().sync();
    }

    @Override
    public void close() {
        try {
            cf.close().await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }

}
