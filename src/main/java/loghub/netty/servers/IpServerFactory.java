package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.channel.Channel;

public abstract class IpServerFactory<CC extends Channel> extends ServerFactory<CC, InetSocketAddress> {

}
