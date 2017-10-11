package loghub.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractIpNettyServer;
import loghub.netty.servers.ServerFactory;

public abstract class NettyIpReceiver<SM> extends NettyReceiver<AbstractIpNettyServer, ServerFactory<ServerSocketChannel, InetSocketAddress>, ServerSocketChannel, SocketChannel, InetSocketAddress, SM> {

    private int port;
    private String host = null;
    private InetSocketAddress addr = null;

    public NettyIpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            addr = new InetSocketAddress(host != null ? InetAddress.getByName(host) : null , port);
        } catch (UnknownHostException e) {
            logger.error("Unknow host to bind: {}", host);
            return false;
        }
        return super.configure(properties);
    }

    @Override
    public InetSocketAddress getListenAddress() {
        return addr;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        // Ensure host is null if given empty string, to be resolved as "bind *" by InetSocketAddress;
        this.host = host != null && !host.isEmpty() ? host : null;
    }

}
