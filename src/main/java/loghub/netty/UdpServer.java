package loghub.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.socket.DatagramChannel;
import loghub.configuration.Properties;

public class UdpServer<F> extends AbstractIpNettyServer<UdpFactory, Bootstrap, Channel, DatagramChannel, DatagramChannel, F> {
    
    protected UdpServer(HandlersSource<DatagramChannel, DatagramChannel> source) {
        super(source);
    }

    @Override
    protected UdpFactory getNewFactory(Properties properties) {
        return new UdpFactory(poller);
    }

}
