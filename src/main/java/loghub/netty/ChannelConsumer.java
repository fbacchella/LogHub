package loghub.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

public interface ChannelConsumer {
    public void addHandlers(ChannelPipeline pipe);
    public default void addOptions(ServerBootstrap bootstrap) { }
    public default void addOptions(Bootstrap bootstrap) { }
    public void exception(ChannelHandlerContext ctx, Throwable cause);
    public void logFatalException(Throwable ex);
}
