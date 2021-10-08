package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.netty.servers.IpServerFactory;

public class TcpFactory extends IpServerFactory<SocketChannel> {

    public TcpFactory(POLLER poller) {
        super(poller);
    }

    @Override
    public ChannelFactory<ServerChannel> getInstance() {
        return this::serverChannelProvider;
    }

}
