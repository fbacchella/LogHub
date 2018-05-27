package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

public interface ChannelConsumer<BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel> {
    public void addHandlers(ChannelPipeline pipe);
    public default void addOptions(BS bootstrap) { }
    public void exception(ChannelHandlerContext ctx, Throwable cause) throws Exception;
}
