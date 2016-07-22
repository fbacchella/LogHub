package loghub.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import loghub.Event;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractNettyServer;

public abstract class NettyIpReceiver<S extends AbstractNettyServer<CF, BS, BSC, SC, InetSocketAddress>, CF extends ComponentFactory<BS, BSC, InetSocketAddress>, BS extends AbstractBootstrap<BS,BSC>, BSC extends Channel, SC extends Channel, CC extends Channel, SM> extends NettyReceiver<S, CF, BS, BSC, SC, CC, InetSocketAddress, SM> {

    private int port;
    private String host = null;
    private InetSocketAddress addr = null;

    public NettyIpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties) {
        try {
            addr = new InetSocketAddress(host !=null ? InetAddress.getByName(host) : null , port);
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
