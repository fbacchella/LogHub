package loghub.netty.transport;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ServerChannel;

public enum POLLER {
    NIO,
    EPOLL,
    OIO,
    KQUEUE,
    IO_URING,
    LOCAL;

    private static final Logger logger = LogManager.getLogger();

    private final AtomicReference<IoHandlerFactory> handlerFactoryReference = new AtomicReference<>();

    public boolean isAvailable() {
        return pollers[ordinal()] != null;
    }

    public boolean isUnixSocket() {
        return pollers[ordinal()].isUnixSocket();
    }

    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        return pollers[ordinal()].serverChannelProvider(transport);
    }

    public Channel clientChannelProvider(TRANSPORT transport) {
        return pollers[ordinal()].clientChannelProvider(transport);
    }

    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new MultiThreadIoEventLoopGroup(threads, threadFactory, getIoHandlerFactory());
    }

    public EventLoopGroup getEventLoopGroup() {
        return new MultiThreadIoEventLoopGroup(getIoHandlerFactory());
    }

    private IoHandlerFactory getIoHandlerFactory() {
        Supplier<IoHandlerFactory> factorySupplier = pollers[ordinal()].getFactorySupplier();
        return handlerFactoryReference.updateAndGet(v -> v == null ? factorySupplier.get() : v);
    }

    public void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        pollers[ordinal()].setKeepAlive(bootstrap, cnt, idle, intvl);
    }

    public void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        pollers[ordinal()].setKeepAlive(bootstrap, cnt, idle, intvl);
    }

    public void setKeepAlive(ChannelConfig config, int cnt, int idle, int intvl) {
        pollers[ordinal()].setKeepAlive(config, cnt, idle, intvl);
    }

    public static final POLLER DEFAULTPOLLER;

    private static final PollerServiceProvider[] pollers = new PollerServiceProvider[POLLER.values().length];

    static {
        ServiceLoader<PollerServiceProvider> serviceLoader = ServiceLoader.load(PollerServiceProvider.class);
        serviceLoader.stream()
                     .map(ServiceLoader.Provider::get)
                     .filter(PollerServiceProvider::isValid)
                     .forEach(psp -> pollers[psp.getPoller().ordinal()] = psp);
        logger.debug("Loaded pollers {}", POLLER::getPollers);
        if (pollers[EPOLL.ordinal()] != null) {
            DEFAULTPOLLER = EPOLL;
        } else if (pollers[KQUEUE.ordinal()] != null) {
            DEFAULTPOLLER =  KQUEUE;
        } else {
            DEFAULTPOLLER =  NIO;
        }
    }

    public static Set<POLLER> getPollers() {
        return Arrays.stream(pollers)
                     .filter(Objects::nonNull)
                     .map(PollerServiceProvider::getPoller)
                     .collect(Collectors.toSet());
    }

    public static POLLER resolve(String pollerName) {
        pollerName = pollerName.toUpperCase(Locale.ENGLISH);
        if ("DEFAULT".equals(pollerName)) {
            return DEFAULTPOLLER;
        } else {
            POLLER poller;
            try {
                poller = POLLER.valueOf(pollerName);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unhandled poller: \"" + pollerName + "\"");
            }
            if (! poller.isAvailable()) {
                throw new IllegalArgumentException("Unavailable poller on this platform: " + poller);
            } else {
                return poller;
            }
        }
    }

}
