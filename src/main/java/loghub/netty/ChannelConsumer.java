package loghub.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

public interface ChannelConsumer {
    void addHandlers(ChannelPipeline pipe);
    default void addOptions(ServerBootstrap bootstrap) { }
    default void addOptions(Bootstrap bootstrap) { }
    void exception(ChannelHandlerContext ctx, Throwable cause);
    void logFatalException(Throwable ex);
}
