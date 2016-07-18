package loghub.netty;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import loghub.Event;
import loghub.Pipeline;

public abstract class NettyIpReceiver<S extends AbstractNettyServer<A, B, C, D, E, F>, A extends ComponentFactory<B, C, D, E>, B extends AbstractBootstrap<B,C>,C extends Channel, D extends Channel, E extends Channel, F> extends NettyReceiver<S, A, B, C, D, E, F> {

    private int port;
    private String host = null;

    public NettyIpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    protected SocketAddress getAddress() {
        try {
            return new InetSocketAddress(host !=null ? InetAddress.getByName(host) :null , port);
        } catch (UnknownHostException e) {
            return null;
        }
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
        this.host = host != null && !host.isEmpty() ? host : null;
    }

}
