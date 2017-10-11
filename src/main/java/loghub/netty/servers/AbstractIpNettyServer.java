package loghub.netty.servers;

import java.net.InetSocketAddress;

import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

public abstract class AbstractIpNettyServer<SC extends > extends AbstractNettyServer<ServerFactory<ServerSocketChannel, InetSocketAddress>, ServerSocketChannel, SocketChannel, InetSocketAddress> {

}
