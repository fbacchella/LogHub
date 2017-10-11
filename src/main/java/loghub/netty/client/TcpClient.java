package loghub.netty.client;

import java.net.InetSocketAddress;

import io.netty.channel.socket.SocketChannel;
import loghub.configuration.Properties;
import loghub.netty.POLLER;

public class TcpClient extends AbstractNettyClient<TcpFactory, SocketChannel, InetSocketAddress> {

    @Override
    protected TcpFactory getNewFactory(Properties properties) {
        return new TcpFactory(POLLER.NIO);
    }

}
