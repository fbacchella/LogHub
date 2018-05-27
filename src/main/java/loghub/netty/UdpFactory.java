package loghub.netty;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpFactory extends IpClientFactory<Channel> {

    private static final ChannelFactory<Channel> niochannelfactory = new ChannelFactory<Channel>() {
        @Override 
        public DatagramChannel newChannel() {
            return new NioDatagramChannel();
        }
    };

    private static final ChannelFactory<Channel> epollchannelfactory = new ChannelFactory<Channel>() {
        @Override 
        public DatagramChannel newChannel() {
            return new EpollDatagramChannel();
        }
    };

    private final POLLER poller;

    public UdpFactory(POLLER poller) {
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
    public ChannelFactory<Channel> getInstance() {
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
