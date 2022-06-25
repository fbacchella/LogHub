package loghub.netty.transport;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollDomainDatagramChannel;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueDomainSocketChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerDomainSocketChannel;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpChannel;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.sctp.oio.OioSctpChannel;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioServerSocketChannel;

public enum POLLER {
    NIO {
        @Override
        boolean isAvailable() {
            return true;
        }
        @Override
        ServerChannel serverChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new NioServerSocketChannel();
            case SCTP: return new NioSctpServerChannel();
            case UDP: throw new UnsupportedOperationException();
            case UNIX_STREAM: throw new UnsupportedOperationException();
            case UNIX_DGRAM: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
            }

        }
        @Override
        Channel clientChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new NioSocketChannel();
            case UDP: return new NioDatagramChannel();
            case SCTP: return new NioSctpChannel();
            case UNIX_STREAM: throw new UnsupportedOperationException();
            case UNIX_DGRAM: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
            }
        }
        @Override
        EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new NioEventLoopGroup(threads, threadFactory);
        }
    },
    EPOLL {
        @Override
        boolean isAvailable() {
            return Epoll.isAvailable();
        }
        @Override
        ServerChannel serverChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new EpollServerSocketChannel();
            case UDP:  throw new UnsupportedOperationException();
            case SCTP: throw new UnsupportedOperationException();
            case UNIX_STREAM: return new EpollServerDomainSocketChannel();
            case UNIX_DGRAM: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
            }
        }
        @Override
        Channel clientChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new EpollServerSocketChannel();
            case UDP: return new EpollDatagramChannel();
            case SCTP: throw new UnsupportedOperationException();
            case UNIX_STREAM:return new EpollDomainSocketChannel();
            case UNIX_DGRAM:  return new EpollDomainDatagramChannel();
            default: throw new IllegalStateException();
            }
        }
        @Override
        EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new EpollEventLoopGroup(threads, threadFactory);
        }
    },
    OIO {
        boolean isAvailable() {
            return false;
        }
        ServerChannel serverChannelProvider(TRANSPORT transport) {
            throw new UnsupportedOperationException("Deprecated OIO");
        }
        Channel clientChannelProvider(TRANSPORT transport) {
            throw new UnsupportedOperationException("Deprecated OIO");
        }

        EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            throw new UnsupportedOperationException("Deprecated OIO");
        }
    },
    KQUEUE {
        @Override
        boolean isAvailable() {
            return KQueue.isAvailable();
        }
        @Override
        ServerChannel serverChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new KQueueServerDomainSocketChannel();
            case UDP: throw new UnsupportedOperationException();
            case SCTP: throw new UnsupportedOperationException();
            case UNIX_STREAM: return new KQueueServerDomainSocketChannel();
            case UNIX_DGRAM: throw new UnsupportedOperationException();
            default: throw new IllegalStateException();
            }
        }
        @Override
        Channel clientChannelProvider(TRANSPORT transport) {
            switch (transport) {
            case TCP: return new KQueueServerSocketChannel();
            case UDP: return new KQueueDatagramChannel();
            case SCTP: throw new UnsupportedOperationException();
            case UNIX_STREAM: return new KQueueDomainSocketChannel();
            case UNIX_DGRAM: return new KQueueDomainSocketChannel();
            default: throw new IllegalStateException();
            }
        }
         @Override
        EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new KQueueEventLoopGroup(threads, threadFactory);
        }
    },
    ;
    abstract boolean isAvailable();
    abstract ServerChannel serverChannelProvider(TRANSPORT transport);
    abstract Channel clientChannelProvider(TRANSPORT transport);
    abstract EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory);
    public static final POLLER DEFAULTPOLLER;
    static {
        if (EPOLL.isAvailable()) {
            DEFAULTPOLLER = EPOLL;
        } else if (KQUEUE.isAvailable()) {
            DEFAULTPOLLER =  KQUEUE;
        } else {
            DEFAULTPOLLER =  NIO;
        }
    }
    public static POLLER resolve(String pollerName) {
        pollerName = pollerName.toUpperCase(Locale.ENGLISH);
        if ("DEFAULT".equals(pollerName)) {
            return DEFAULTPOLLER;
        } else {
            POLLER poller = POLLER.valueOf(pollerName);
            if (! poller.isAvailable()) {
                throw new IllegalArgumentException("Unavailable poller on this platform: " + poller);
            } else {
                return poller;
            }
        }
    }

}
