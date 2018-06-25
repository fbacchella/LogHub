package loghub.netty.servers;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import loghub.netty.TcpFactory;

public class AbstractTcpServer<S extends AbstractTcpServer<S, B>,
                               B extends AbstractTcpServer.Builder<S, B>
                              > extends NettyIpServer<TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, S, B> {

    public abstract static class Builder<S extends AbstractTcpServer<S, B>,
                                         B extends AbstractTcpServer.Builder<S, B>
                                        > extends NettyIpServer.Builder<S, B, ServerBootstrap, ServerChannel> {
        protected Builder() {
        }
    }

    protected AbstractTcpServer(B builder) throws IllegalArgumentException, InterruptedException {
        super(builder);
    }

    Channel listeningChannel;

    @Override
    protected void makeChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> bootstrap, InetSocketAddress address, B builder) throws IllegalStateException, InterruptedException {
        // Bind and start to accept incoming connections.
        try {
            ChannelFuture cf = bootstrap.bind(address);
            cf.get();
            listeningChannel = cf.channel();
            logger.debug("bond to {}", address);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Failed to start listening on " +  address, e.getCause());
        }
    }

    @Override
    public void configureBootStrap(ServerBootstrap bootstrap, B builder) {
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        super.configureBootStrap(bootstrap, builder);
    }

    @Override
    protected TcpFactory getNewFactory() {
        return new TcpFactory(poller);
    }

    @Override
    public void waitClose() throws InterruptedException {
        listeningChannel.closeFuture().sync();
    }

    @Override
    public void close() {
        try {
            listeningChannel.close().await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        super.close();
    }

}
