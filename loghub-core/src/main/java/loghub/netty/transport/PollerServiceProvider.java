package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

public interface PollerServiceProvider {
    POLLER getPoller();
    boolean isValid();
    boolean isUnixSocket();
    ServerChannel serverChannelProvider(TRANSPORT transport);
    Channel clientChannelProvider(TRANSPORT transport);
    EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory);
    EventLoopGroup getEventLoopGroup();
    default void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        // default does nothing
    }
    default void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        // default does nothing
    }
}
