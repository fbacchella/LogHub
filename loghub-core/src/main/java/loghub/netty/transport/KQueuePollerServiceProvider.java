package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueDomainDatagramChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import loghub.Helpers;

public class KQueuePollerServiceProvider implements PollerServiceProvider {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalServerChannel();
        case TCP: return new KQueueServerSocketChannel();
        case UNIX_STREAM: return new KQueueServerDomainSocketChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalChannel();
        case TCP: return new KQueueSocketChannel();
        case UDP: return new KQueueDatagramChannel();
        case UNIX_STREAM: return new KQueueDomainSocketChannel();
        case UNIX_DGRAM: return new KQueueDomainDatagramChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }

    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new KQueueEventLoopGroup(threads, threadFactory);
    }

    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new KQueueEventLoopGroup();
    }

    @Override
    public POLLER getPoller() {
        return POLLER.KQUEUE;
    }

    @Override
    public boolean isValid() {
        if (KQueue.isAvailable()) {
            return true;
        } else {
            logger.warn("KQueue not available: {}", Helpers.resolveThrowableException(KQueue.unavailabilityCause()));
            return false;
        }
    }

    @Override
    public boolean isUnixSocket() {
        return true;
    }

}
