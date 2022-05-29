package loghub.netty;

import io.netty.channel.ChannelFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.SocketChannel;

public class TcpServerHandler extends IpServerHandler<SocketChannel> {

    public TcpServerHandler(POLLER poller) {
        super(poller);
    }

    @Override
    public ChannelFactory<ServerChannel> getInstance() {
        return this::serverChannelProvider;
    }

}
