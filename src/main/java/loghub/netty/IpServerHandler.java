package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public abstract class IpServerHandler<CC extends Channel> extends ServerHandler<CC, InetSocketAddress> {

    public IpServerHandler(POLLER poller) {
        super(poller);
    }

}
