package loghub.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.socket.DatagramChannel;

public class UdpHandler extends IpClientHandler<DatagramChannel> {

    public UdpHandler(POLLER poller) {
        super(poller);
    }

    @Override
    public ChannelFactory<Channel> getInstance() {
        return this::datagramChannelProvider;
    }

}
