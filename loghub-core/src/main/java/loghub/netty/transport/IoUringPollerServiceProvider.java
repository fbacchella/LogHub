package loghub.netty.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringChannelOption;
import io.netty.incubator.channel.uring.IOUringDatagramChannel;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import loghub.Helpers;

public class IoUringPollerServiceProvider implements PollerServiceProvider {

    private static final Logger logger = LogManager.getLogger();

    private static final byte IO_URING_ACCESSIBLE;
    static {
        byte accessible;
        try {
            Path ioUringDisabledPath = Path.of("/proc/sys/kernel/io_uring_disabled");
            if (Files.exists(ioUringDisabledPath)) {
                String disabledString = Files.readAllLines(ioUringDisabledPath).get(0);
                accessible = Byte.parseByte(disabledString);
            } else {
                accessible = -2;
            }
        } catch (IOException | NumberFormatException e) {
            logger.warn("io_uring status can't be identified: {}", () -> Helpers.resolveThrowableException(e));
            accessible = -1;
        }
        IO_URING_ACCESSIBLE = accessible;
    }

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        if (Objects.requireNonNull(transport) == TRANSPORT.TCP) {
            return new IOUringServerSocketChannel();
        } else {
            throw new UnsupportedOperationException(transport.name());
        }
    }
    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case TCP: return new IOUringSocketChannel();
        case UDP: return new IOUringDatagramChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new IOUringEventLoopGroup(threads, threadFactory);
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new IOUringEventLoopGroup();
    }

    @Override
    public POLLER getPoller() {
        return POLLER.IO_URING;
    }

    @Override
    public boolean isValid() {
        if (IOUring.isAvailable()) {
            return IO_URING_ACCESSIBLE >= -1 && IO_URING_ACCESSIBLE <= 1;
        } else {
            logger.warn("io_uring not available: {}", Helpers.resolveThrowableException(IOUring.unavailabilityCause()));
            return false;
        }
    }

    @Override
    public boolean isUnixSocket() {
        return true;
    }

    @Override
    public void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.childOption(IOUringChannelOption.TCP_KEEPCNT, 3);
        bootstrap.childOption(IOUringChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.childOption(IOUringChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.option(IOUringChannelOption.TCP_KEEPCNT, 3);
        bootstrap.option(IOUringChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.option(IOUringChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(ChannelConfig config, int cnt, int idle, int intvl) {
        config.setOption(IOUringChannelOption.TCP_KEEPCNT, 3);
        config.setOption(IOUringChannelOption.TCP_KEEPIDLE, 60);
        config.setOption(IOUringChannelOption.TCP_KEEPINTVL, 10);
    }

}
