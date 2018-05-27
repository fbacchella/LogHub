package loghub.netty;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import loghub.netty.servers.IpServerFactory;

public class TcpFactory extends IpServerFactory<SocketChannel> {

    private static final ChannelFactory<ServerChannel> niochannelfactory = new ChannelFactory<ServerChannel>() {
        @Override 
        public ServerChannel newChannel() {
            return new NioServerSocketChannel();
        }
    };

    private static final ChannelFactory<ServerChannel> epollchannelfactory = new ChannelFactory<ServerChannel>() {
        @Override 
        public ServerChannel newChannel() {
            return new EpollServerSocketChannel();
        }
    };

    private final POLLER poller;

    public TcpFactory(POLLER poller) {
        this.poller = poller;
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        switch (poller) {
        case NIO:
            return new NioEventLoopGroup(threads, threadFactory);
        case EPOLL:
            return new EpollEventLoopGroup(threads, threadFactory);
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public ChannelFactory<ServerChannel> getInstance() {
        switch (poller) {
        case NIO:
            return niochannelfactory;
        case EPOLL:
            return epollchannelfactory;
        default:
            throw new UnsupportedOperationException();
        }
    }

}
