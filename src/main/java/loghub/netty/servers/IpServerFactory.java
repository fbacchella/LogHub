package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;
import loghub.netty.POLLER;

public abstract class IpServerFactory<CC extends Channel> extends ServerFactory<CC, InetSocketAddress> {

    public IpServerFactory(POLLER poller) {
        super(poller);
    }

}
