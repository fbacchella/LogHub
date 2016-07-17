package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TcpFactory extends ServerFactory<ServerSocketChannel, SocketChannel> {

    public enum POLLER {
        NIO,
        EPOLL,
        OIO
    }

    private static final ChannelFactory<ServerSocketChannel> niochannelfactory = new ChannelFactory<ServerSocketChannel>() {
        @Override 
        public ServerSocketChannel newChannel() {
            return new NioServerSocketChannel();
        }
    };

    private final POLLER poller;

    TcpFactory(POLLER poller) {
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
    public ChannelFactory<ServerSocketChannel> getInstance() {
        switch (poller) {
        case NIO:
            return niochannelfactory;
        default:
            throw new UnsupportedOperationException();
        }
    }

}
