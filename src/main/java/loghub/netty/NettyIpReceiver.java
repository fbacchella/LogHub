package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import loghub.ConnectionContext;
import loghub.Event;
import loghub.IpConnectionContext;
import loghub.Pipeline;
import loghub.configuration.Properties;
import loghub.netty.servers.NettyIpServer;
import loghub.security.ssl.ClientAuthentication;

public abstract class NettyIpReceiver<R extends NettyIpReceiver<R, S, B, CF, BS, BSC, SC, CC, SM>,
                                      S extends NettyIpServer<CF, BS, BSC, SC, S, B>,
                                      B extends NettyIpServer.Builder<S, B>,
                                      CF extends ComponentFactory<BS, BSC, InetSocketAddress>,
                                      BS extends AbstractBootstrap<BS,BSC>,
                                      BSC extends Channel,
                                      SC extends Channel,
                                      CC extends Channel,
                                      SM> extends NettyReceiver<R, S, B, CF, BS, BSC, SC, CC, InetSocketAddress, SM> {

    private int port;
    private String host = null;

    public NettyIpReceiver(BlockingQueue<Event> outQueue, Pipeline pipeline) {
        super(outQueue, pipeline);
    }

    @Override
    public boolean configure(Properties properties, B builder) {
        if (isWithSSL()) {
            builder.setSSLClientAuthentication(ClientAuthentication.valueOf(getSSLClientAuthentication().toUpperCase()))
                   .setSSLContext(properties.ssl)
                   .setSSLKeyAlias(getSSLKeyAlias())
                   .useSSL(isWithSSL());
        }
        builder.setPort(port)
               .setHost(host);
        return super.configure(properties, builder);
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

     @Override
     public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, SM message) {
         InetSocketAddress remoteaddr = null;
         InetSocketAddress localaddr = null;
         SocketAddress remoteddr = ctx.channel().remoteAddress();
         SocketAddress localddr = ctx.channel().localAddress();
         if (remoteddr instanceof InetSocketAddress) {
             remoteaddr = (InetSocketAddress)remoteddr;
         }
         if (localddr instanceof InetSocketAddress) {
             remoteaddr = (InetSocketAddress)remoteddr;
         }
         return new IpConnectionContext(localaddr, remoteaddr, null);
     }

}
