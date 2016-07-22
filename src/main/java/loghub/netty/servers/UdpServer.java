package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;
import loghub.configuration.Properties;
import loghub.netty.UdpFactory;

public class UdpServer extends AbstractNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, InetSocketAddress> {

    @Override
    protected UdpFactory getNewFactory(Properties properties) {
        return new UdpFactory(poller);
    }

}
