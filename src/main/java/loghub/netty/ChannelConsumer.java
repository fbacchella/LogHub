package loghub.netty;

import java.net.SocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

public interface ChannelConsumer<BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel, SA extends SocketAddress> {
    public void addHandlers(ChannelPipeline pipe);
    public default void addOptions(BS bootstrap) { }
    public default void exception(ChannelHandlerContext ctx, Throwable cause) { }
    public SA getListenAddress();
}
