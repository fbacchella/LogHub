package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpFactory extends ClientFactory<DatagramChannel, DatagramChannel> {

    private static final ChannelFactory<DatagramChannel> niochannelfactory = new ChannelFactory<DatagramChannel>() {
        @Override 
        public DatagramChannel newChannel() {
            return new NioDatagramChannel();
        }
    };

    private final POLLER poller;

    UdpFactory(POLLER poller) {
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
    public ChannelFactory<DatagramChannel> getInstance() {
        switch (poller) {
        case NIO:
            return niochannelfactory;
        default:
            throw new UnsupportedOperationException();
        }
    }

}
