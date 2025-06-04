package loghub.netty.transport;

import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.IoHandlerFactory;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.ServerChannel;

public interface PollerServiceProvider {

    POLLER getPoller();
    boolean isValid();
    boolean isUnixSocket();
    ServerChannel serverChannelProvider(TRANSPORT transport);
    Channel clientChannelProvider(TRANSPORT transport);
    default EventLoopGroup getEventLoopGroup(int threads, ThreadFactory threadFactory) {
        return new MultiThreadIoEventLoopGroup(threads, threadFactory, getIoHandlerFactory());
    }
    default EventLoopGroup getEventLoopGroup() {
        return new MultiThreadIoEventLoopGroup(getIoHandlerFactory());
    }
    IoHandlerFactory getIoHandlerFactory();
    default void setKeepAlive(ServerBootstrap bootstrap, int cnt, int idle, int intvl) {
        // default does nothing
    }
    default void setKeepAlive(Bootstrap bootstrap, int cnt, int idle, int intvl) {
        // default does nothing
    }
    default void setKeepAlive(ChannelConfig config, int cnt, int idle, int intvl) {
        // default does nothing
    }
    default IoHandlerFactory makeFactory(Supplier<IoHandlerFactory> newFactory) {
        return getPoller().getHandlerFactoryReference().updateAndGet(v -> v == null ? newFactory.get() : v);
    }
}
