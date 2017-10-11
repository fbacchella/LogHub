package loghub.netty.client;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import loghub.netty.POLLER;

public class TcpFactory extends ClientFactory<SocketChannel, InetSocketAddress> {

    private static final ChannelFactory<Channel> niochannelfactory = new ChannelFactory<Channel>() {
        @Override 
        public SocketChannel newChannel() {
            return new NioSocketChannel();
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
    public ChannelFactory<Channel> getInstance() {
        switch (poller) {
        case NIO:
            return niochannelfactory;
        default:
            throw new UnsupportedOperationException();
        }
    }

}
