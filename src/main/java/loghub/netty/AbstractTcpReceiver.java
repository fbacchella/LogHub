package loghub.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.net.ssl.SSLSession;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
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

public abstract class AbstractTcpReceiver<R extends AbstractTcpReceiver<R, S, B>,
                                          S extends AbstractTcpServer<S, B>,
                                          B extends AbstractTcpServer.Builder<S, B>
                                         > extends NettyIpReceiver<R, S, B, TcpFactory, ServerBootstrap, ServerChannel, ServerSocketChannel, SocketChannel, ByteBuf> {

    private int backlog = 16;

    public AbstractTcpReceiver() {
        super();
    }

    @Override
    public boolean configure(Properties properties, B builder) {
        builder.setBacklog(backlog);
        return super.configure(properties, builder);
    }

    @Override
    public ByteBuf getContent(ByteBuf message) {
        return message;
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, ByteBuf message) {
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

    /**
     * @return the backlog
     */
     public int getListenBacklog() {
        return backlog;
    }

    /**
     * @param backlog the backlog to set
     */
     public void setListenBacklog(int backlog) {
         this.backlog = backlog;
     }

}
