package loghub.netty.transport;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.ServerChannel;
import io.netty.channel.uring.IoUring;
import io.netty.channel.uring.IoUringChannelOption;
import io.netty.channel.uring.IoUringDatagramChannel;
import io.netty.channel.uring.IoUringIoHandler;
import io.netty.channel.uring.IoUringServerSocketChannel;
import io.netty.channel.uring.IoUringSocketChannel;
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
            logger.info("io_uring status can't be identified: {}", () -> Helpers.resolveThrowableException(e));
            accessible = -1;
        }
        IO_URING_ACCESSIBLE = accessible;
    }

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        if (transport == TRANSPORT.TCP) {
            return new IoUringServerSocketChannel();
        } else {
            throw new UnsupportedOperationException(transport.name());
        }
    }
    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case TCP: return new IoUringSocketChannel();
        case UDP: return new IoUringDatagramChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public IoHandlerFactory getIoHandlerFactory() {
        return makeFactory(IoUringIoHandler::newFactory);
    }

    @Override
    public POLLER getPoller() {
        return POLLER.IO_URING;
    }

    @Override
    public boolean isValid() {
        if (IoUring.isAvailable()) {
            return IO_URING_ACCESSIBLE >= -1 && IO_URING_ACCESSIBLE <= 1;
        } else {
            logger.info("io_uring not available: {}", () -> Helpers.resolveThrowableException(IoUring.unavailabilityCause()));
            return false;
        }
    }

    @Override
    public boolean isUnixSocket() {
        return true;
    }

    @Override
    public void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.childOption(IoUringChannelOption.TCP_KEEPCNT, 3);
        bootstrap.childOption(IoUringChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.childOption(IoUringChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        bootstrap.option(IoUringChannelOption.TCP_KEEPCNT, 3);
        bootstrap.option(IoUringChannelOption.TCP_KEEPIDLE, 60);
        bootstrap.option(IoUringChannelOption.TCP_KEEPINTVL, 10);
    }

    @Override
    public void setKeepAlive(ChannelConfig config, int cnt, int idle, int intvl) {
        config.setOption(IoUringChannelOption.TCP_KEEPCNT, 3);
        config.setOption(IoUringChannelOption.TCP_KEEPIDLE, 60);
        config.setOption(IoUringChannelOption.TCP_KEEPINTVL, 10);
    }

}
