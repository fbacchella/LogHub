package loghub.netty.transport;

import java.net.InetSocketAddress;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollChannelOption;
import loghub.ConnectionContext;

public class TcpTransport
        extends NettyTransport<InetSocketAddress, Object>
        implements IpServices {

    TcpTransport(POLLER poller) {
        super(poller, TRANSPORT.TCP);
    }

    @Override
    public void configureServerBootStrap(ServerBootstrap bootstrap, TransportConfig config) {
        super.configureServerBootStrap(bootstrap, config);
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
        if (config.getBacklog() >= 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, config.getBacklog());
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
    protected void configureBootStrap(Bootstrap bootstrap, TransportConfig config) {
        IpServices.super.configureAbstractBootStrap(bootstrap, config);
        super.configureBootStrap(bootstrap, config);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        if (poller == POLLER.EPOLL) {
            bootstrap.option(EpollChannelOption.TCP_KEEPCNT, 3);
            bootstrap.option(EpollChannelOption.TCP_KEEPIDLE , 60);
            bootstrap.option(EpollChannelOption.TCP_KEEPINTVL , 10);
        }
        if (config.getTimeout() >= 0) {
            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, config.getTimeout());
        }
    }

    @Override
    public InetSocketAddress resolveAddress(TransportConfig config) {
        return IpServices.super.resolveAddress(config);
    }

    @Override
    public ConnectionContext<InetSocketAddress> getNewConnectionContext(ChannelHandlerContext ctx, Object message) {
        return IpServices.super.getNewConnectionContext(ctx);
    }

}
