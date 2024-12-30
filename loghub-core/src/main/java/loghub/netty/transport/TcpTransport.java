package loghub.netty.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.net.ssl.SSLSession;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.util.Attribute;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;
import lombok.Setter;

@TransportEnum(TRANSPORT.TCP)
public class TcpTransport extends AbstractIpTransport<Object, TcpTransport, TcpTransport.Builder> {

    @Setter
    public static class Builder extends AbstractIpTransport.Builder<Object, TcpTransport, TcpTransport.Builder> {
        protected boolean noDelay = false;
        @Override
        public TcpTransport build() {
            return new TcpTransport(this);
        }
    }
    public static TcpTransport.Builder getBuilder() {
        return new TcpTransport.Builder();
    }

    protected final boolean noDelay;

    public TcpTransport(TcpTransport.Builder builder) {
        super(builder);
        this.noDelay = builder.noDelay;
    }

    @Override
    public void configureServerBootStrap(ServerBootstrap bootstrap) {
        super.configureServerBootStrap(bootstrap);
        if (getBacklog() >= 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, getBacklog());
        }
        bootstrap.option(ChannelOption.TCP_NODELAY, noDelay);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        poller.setKeepAlive(bootstrap, 3, 60, 10);
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap) {
        super.configureBootStrap(bootstrap);
        bootstrap.option(ChannelOption.TCP_NODELAY, noDelay);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        poller.setKeepAlive(bootstrap, 3, 60, 10);
        if (timeout >= 0) {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, timeout);
        }
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, Object message) {
        SocketAddress remoteChannelAddr = ctx.channel().remoteAddress();
        SocketAddress localChannelAddr = ctx.channel().localAddress();
        InetSocketAddress remoteaddr = null;
        InetSocketAddress localaddr = null;
        if (remoteChannelAddr instanceof InetSocketAddress) {
            remoteaddr = (InetSocketAddress) remoteChannelAddr;
        }
        if (localChannelAddr instanceof InetSocketAddress) {
            localaddr = (InetSocketAddress) localChannelAddr;
        }

        Attribute<SSLSession> sess = ctx.channel().attr(SSLSESSIONATTRIBUTE);
        return new IpConnectionContext(localaddr, remoteaddr, sess.get());
    }

}
