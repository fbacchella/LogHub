package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import loghub.netty.servers.ServerFactory;

public class TcpFactory extends ServerFactory<SocketChannel, InetSocketAddress> {

    private static final ChannelFactory<ServerChannel> niochannelfactory = new ChannelFactory<ServerChannel>() {
        @Override 
        public ServerChannel newChannel() {
            return new NioServerSocketChannel();
        }
    };

    private final POLLER poller;

    public TcpFactory(POLLER poller) {
        this.poller = poller;
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        switch (poller) {
        case NIO:
            return new NioEventLoopGroup();
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ChannelFactory<ServerChannel> getInstance() {
        switch (poller) {
        case NIO:
            return niochannelfactory;
        default:
            throw new UnsupportedOperationException();
        }
    }

}
