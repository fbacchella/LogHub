package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollDomainDatagramChannel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import loghub.Helpers;

public class EpollPollerServiceProvider implements PollerServiceProvider {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalServerChannel();
        case TCP: return new EpollServerSocketChannel();
        case UNIX_STREAM: return new EpollServerDomainSocketChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalChannel();
        case TCP: return new EpollSocketChannel();
        case UDP: return new EpollDatagramChannel();
        case UNIX_STREAM: return new EpollDomainSocketChannel();
        case UNIX_DGRAM: return new EpollDomainDatagramChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new EpollEventLoopGroup(threads, threadFactory);
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new EpollEventLoopGroup();
    }

    @Override
    public POLLER getPoller() {
        return POLLER.EPOLL;
    }

    @Override
    public boolean isValid() {
        if (Epoll.isAvailable()) {
            return true;
        } else {
            logger.warn("Epoll not available: {}", Helpers.resolveThrowableException(Epoll.unavailabilityCause()));
            return false;
        }
    }

    @Override
    public boolean isUnixSocket() {
        return true;
    }

    @Override
    public void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.childOption(EpollChannelOption.TCP_KEEPCNT, 3);
        bootstrap.childOption(EpollChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.childOption(EpollChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.option(EpollChannelOption.TCP_KEEPCNT, 3);
        bootstrap.option(EpollChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.option(EpollChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(ChannelConfig config, int cnt, int idle, int intvl) {
        config.setOption(EpollChannelOption.TCP_KEEPCNT, 3);
        config.setOption(EpollChannelOption.TCP_KEEPIDLE, 60);
        config.setOption(EpollChannelOption.TCP_KEEPINTVL, 10);
    }

}
