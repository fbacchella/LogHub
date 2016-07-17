package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpNioFactory extends ClientFactory<DatagramChannel, DatagramChannel> {

    private static final ChannelFactory<DatagramChannel> channelfactory = new ChannelFactory<DatagramChannel>() {
        @Override 
        public DatagramChannel newChannel() {
            return new NioDatagramChannel();
        }
    };

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new NioEventLoopGroup();
    }

    @Override
    public ChannelFactory<DatagramChannel> getInstance() {
        return channelfactory;
    }

}
