package loghub.netty;

import java.net.SocketAddress;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;

public interface ChannelConsumer<BS extends AbstractBootstrap<BS, BSC>, BSC extends Channel, SA extends SocketAddress> {
    public void addHandlers(ChannelPipeline pipe);
    public default void addOptions(BS bootstrap) { }
    public SA getListenAddress();
}
