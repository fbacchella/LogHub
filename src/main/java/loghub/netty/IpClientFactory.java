package loghub.netty;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public abstract class IpClientFactory<CC extends Channel> extends ClientFactory<CC, InetSocketAddress> {

}
