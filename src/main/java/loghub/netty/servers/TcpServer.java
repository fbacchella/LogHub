package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import loghub.netty.TcpFactory;

public class TcpServer extends AbstractNettyServer<TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, InetSocketAddress> {

    Channel cf;

    @Override
    protected boolean makeChannel(AbstractBootstrap<ServerBootstrap, ServerChannel> bootstrap, InetSocketAddress address) {
        // Bind and start to accept incoming connections.
        try {
            cf = bootstrap.bind(address).await().channel();
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    protected TcpFactory getNewFactory() {
        return new TcpFactory(poller);
    }

    @Override
    public void waitClose() throws InterruptedException {
        cf.closeFuture().sync();
    }

}
