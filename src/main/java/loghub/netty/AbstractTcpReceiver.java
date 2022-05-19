package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.net.ssl.SSLSession;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ServerChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.Attribute;
import loghub.ConnectionContext;
import loghub.IpConnectionContext;
import loghub.configuration.Properties;
import loghub.netty.servers.AbstractTcpServer;
import loghub.netty.servers.NettyIpServer;
import loghub.receivers.Blocking;
import lombok.Getter;
import lombok.Setter;

@Blocking
public abstract class AbstractTcpReceiver<R extends AbstractTcpReceiver<R, S, B, SM>,
                                          S extends AbstractTcpServer<S, B>,
                                          B extends AbstractTcpServer.Builder<S, B>,
                                          SM
                                         > extends NettyIpReceiver<R, S, B, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, SM> {

    public abstract static class Builder<B extends AbstractTcpReceiver<?, ?, ?, ?>> extends NettyIpReceiver.Builder<B> {
        @Setter
        private int backlog = -1;
    }

    @Getter
    private final int backlog;

    protected AbstractTcpReceiver(Builder<? extends AbstractTcpReceiver<R, S, B, SM>> builder) {
        super(builder);
        this.backlog = builder.backlog;
    }

    @Override
    public boolean configure(Properties properties, B builder) {
        builder.setBacklog(backlog);
        return super.configure(properties, builder);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, SM message) {
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

        Attribute<SSLSession> sess = ctx.channel().attr(NettyIpServer.SSLSESSIONATTRIBUTE);
        return new IpConnectionContext(localaddr, remoteaddr, sess.get());
    }

}
