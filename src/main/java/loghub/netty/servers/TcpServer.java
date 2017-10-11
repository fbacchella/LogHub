package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.configuration.Properties;

public class TcpServer extends AbstractNettyServer<TcpFactory, ServerSocketChannel, SocketChannel, InetSocketAddress> {

    @Override
    protected TcpFactory getNewFactory(Properties properties) {
        return new TcpFactory(poller);
    }

}
