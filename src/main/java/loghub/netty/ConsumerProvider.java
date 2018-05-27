package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;

public interface ConsumerProvider<R extends NettyReceiver<?, ?, ?, ?, BS, BSC, ?, ?, ?, ?>,
                                  BS extends AbstractBootstrap<BS,BSC>, 
                                  BSC extends Channel
                                 > {
    public ChannelConsumer<BS, BSC> getConsumer();
}
