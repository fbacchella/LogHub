package loghub.netty;

import java.util.Locale;
import java.util.concurrent.ThreadFactory;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueDatagramChannel;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public enum POLLER {
    NIO {
        @Override
        public boolean isAvailable() {
            return true;
        }
        @Override
        public ServerChannel serverChannelProvider() {
            return new NioServerSocketChannel();
        }
        @Override
        public SocketChannel clientChannelProvider() {
            return new NioSocketChannel();
        }
        @Override
        public DatagramChannel datagramChannelProvider() {
            return new NioDatagramChannel();
        }
        @Override
        public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new NioEventLoopGroup(threads, threadFactory);
        }
    },
    EPOLL {
        @Override
        public boolean isAvailable() {
            return Epoll.isAvailable();
        }
        @Override
        public ServerChannel serverChannelProvider() {
            return new EpollServerSocketChannel();
        }
        @Override
        public DatagramChannel datagramChannelProvider() {
            return new EpollDatagramChannel();
        }
        @Override
        public SocketChannel clientChannelProvider() {
            return new EpollSocketChannel();
        }
        @Override
        public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new EpollEventLoopGroup(threads, threadFactory);
        }
    },
    OIO {
        public boolean isAvailable() {
            return false;
        }
        public ServerChannel serverChannelProvider() {
            throw new UnsupportedOperationException();
        }
        public SocketChannel clientChannelProvider() {
            throw new UnsupportedOperationException();
        }
        public DatagramChannel datagramChannelProvider() {
            throw new UnsupportedOperationException();
        }
        public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            throw new UnsupportedOperationException();
        }
    },
    KQUEUE {
        @Override
        public boolean isAvailable() {
            return KQueue.isAvailable();
        }
        @Override
        public ServerChannel serverChannelProvider() {
            return new KQueueServerSocketChannel();
        }
        @Override
        public SocketChannel clientChannelProvider() {
            return new KQueueSocketChannel();
        }
        @Override
        public DatagramChannel datagramChannelProvider() {
            return new KQueueDatagramChannel();
        }
        @Override
        public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
            return new KQueueEventLoopGroup(threads, threadFactory);
        }
    },
    ;
    public abstract boolean isAvailable();
    public abstract ServerChannel serverChannelProvider();
    public abstract SocketChannel clientChannelProvider();
    public abstract DatagramChannel datagramChannelProvider();
    public abstract EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory);
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
                throw new IllegalArgumentException("Unavailable poller on this plateform: " + poller);
            } else {
                return poller;
            }
        }
    }
}
