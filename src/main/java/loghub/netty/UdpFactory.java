package loghub.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.socket.DatagramChannel;

public class UdpFactory extends IpClientFactory<DatagramChannel> {

    public UdpFactory(POLLER poller) {
        super(poller);
    }

    @Override
    public ChannelFactory<Channel> getInstance() {
        return this::datagramChannelProvider;
    }

}
