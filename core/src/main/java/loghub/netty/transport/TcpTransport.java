package loghub.netty.transport;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.net.ssl.SSLSession;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.util.Attribute;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;

@TransportEnum(TRANSPORT.TCP)
public class TcpTransport extends AbstractIpTransport<Object, TcpTransport, TcpTransport.Builder> {

    public static class Builder extends AbstractIpTransport.Builder<Object, TcpTransport, TcpTransport.Builder> {
        @Override
        public TcpTransport build() {
            return new TcpTransport(this);
        }
    }
    public static TcpTransport.Builder getBuilder() {
        return new TcpTransport.Builder();
    }

    public TcpTransport(TcpTransport.Builder builder) {
        super(builder);
    }

    @Override
    public void configureServerBootStrap(ServerBootstrap bootstrap) {
        super.configureServerBootStrap(bootstrap);
        super.configureAbstractBootStrap(bootstrap);
        if (getBacklog() >= 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, getBacklog());
        }
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        if (poller == POLLER.EPOLL) {
            bootstrap.childOption(EpollChannelOption.TCP_KEEPCNT, 3);
            bootstrap.childOption(EpollChannelOption.TCP_KEEPIDLE , 60);
            bootstrap.childOption(EpollChannelOption.TCP_KEEPINTVL , 10);
        }
    }

    @Override
    protected void configureBootStrap(Bootstrap bootstrap) {
        super.configureAbstractBootStrap(bootstrap);
        super.configureBootStrap(bootstrap);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        if (poller == POLLER.EPOLL) {
            bootstrap.option(EpollChannelOption.TCP_KEEPCNT, 3);
            bootstrap.option(EpollChannelOption.TCP_KEEPIDLE , 60);
            bootstrap.option(EpollChannelOption.TCP_KEEPINTVL , 10);
        }
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
            remoteaddr = (InetSocketAddress)remoteChannelAddr;
        }
        if (localChannelAddr instanceof InetSocketAddress) {
            localaddr = (InetSocketAddress)localChannelAddr;
        }

        Attribute<SSLSession> sess = ctx.channel().attr(SSLSESSIONATTRIBUTE);
        return new IpConnectionContext(localaddr, remoteaddr, sess.get());
    }

}
