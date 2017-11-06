package loghub.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpFactory extends ClientFactory<Channel, InetSocketAddress> {

    private static final ChannelFactory<Channel> niochannelfactory = new ChannelFactory<Channel>() {
        @Override 
        public DatagramChannel newChannel() {
            return new NioDatagramChannel();
        }
    };

    private final POLLER poller;

    public UdpFactory(POLLER poller) {
        this.poller = poller;
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadName) {
        switch (poller) {
        case NIO:
            return new NioEventLoopGroup(threads, threadName);
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
