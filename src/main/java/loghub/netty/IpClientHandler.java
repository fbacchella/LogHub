package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public abstract class IpClientHandler<CC extends Channel> extends ClientHandler<CC, InetSocketAddress> {

    public IpClientHandler(POLLER poller) {
        super(poller);
    }

}
