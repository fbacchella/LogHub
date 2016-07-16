package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;

public abstract class ClientFactory extends ComponentFactory<Bootstrap, Channel> {

    private EventLoopGroup workerGroup;
    private Bootstrap bootstrap;

    @Override
    public AbstractBootstrap<Bootstrap, Channel> getBootStrap() {
        bootstrap = new Bootstrap();
        bootstrap.channelFactory(getInstance());
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    @Override
    public void group() {
        workerGroup = getEventLoopGroup();
        bootstrap.group(workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
    }

    @Override
    public boolean withChildHandler() {
        return false;
    }

    @Override
    public void addChildhandlers(HandlersSource source) {
        throw new UnsupportedOperationException("Client don't handle child");
    }

}
