package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;
import loghub.configuration.Properties;

public class UdpServer extends AbstractNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, InetSocketAddress> {

    @Override
    protected UdpFactory getNewFactory(Properties properties) {
        return new UdpFactory(poller);
    }

}
