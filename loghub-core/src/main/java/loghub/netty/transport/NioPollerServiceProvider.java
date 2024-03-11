package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.sctp.nio.NioSctpChannel;
import io.netty.channel.sctp.nio.NioSctpServerChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NioPollerServiceProvider implements PollerServiceProvider {
    @Override
    public ServerChannel serverChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalServerChannel();
        case TCP: return new NioServerSocketChannel();
        case SCTP: return new NioSctpServerChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }
    @Override
    public Channel clientChannelProvider(TRANSPORT transport) {
        switch (transport) {
        case LOCAL: return new LocalChannel();
        case TCP: return new NioSocketChannel();
        case UDP: return new NioDatagramChannel();
        case SCTP: return new NioSctpChannel();
        default: throw new UnsupportedOperationException(transport.name());
        }
    }
    @Override
    public EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new NioEventLoopGroup(threads, threadFactory);
    }
    @Override
    public EventLoopGroup getEventLoopGroup() {
        return new NioEventLoopGroup();
    }
    @Override
    public POLLER getPoller() {
        return POLLER.NIO;
    }

    @Override
    public boolean isValid() {
        return true;
    }
    @Override
    public boolean isUnixSocket() {
        return false;
    }

}
