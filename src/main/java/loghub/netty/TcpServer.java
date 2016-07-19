package loghub.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.configuration.Properties;

public class TcpServer<F> extends AbstractIpNettyServer<TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, F> {

    @Override
    protected TcpFactory getNewFactory(Properties properties) {
        return new TcpFactory(poller);
    }

}
