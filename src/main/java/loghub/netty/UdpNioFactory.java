package loghub.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class UdpNioFactory extends ClientFactory {

    private static final ChannelFactory<Channel> channelfactory = new ChannelFactory<Channel>() {
        @Override 
        public Channel newChannel() {
            return new NioDatagramChannel();
        }
    };

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new NioEventLoopGroup();
    }

    @Override
    public ChannelFactory<Channel> getInstance() {
        return channelfactory;
    }

}
