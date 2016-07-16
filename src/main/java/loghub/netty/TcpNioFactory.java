package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class TcpNioFactory extends ServerFactory {

    private static final ChannelFactory<ServerChannel> channelfactory = new ChannelFactory<ServerChannel>() {
        @Override 
        public ServerChannel newChannel() {
            return new NioServerSocketChannel();
        }
    };

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new NioEventLoopGroup();
    }

    @Override
    public ChannelFactory<ServerChannel> getInstance() {
        return channelfactory;
    }

}
