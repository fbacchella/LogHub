package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;

public abstract class ClientFactory<D extends Channel, E extends Channel> extends ComponentFactory<Bootstrap, Channel, D, E> {

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
    public void addChildhandlers(HandlersSource<D, E> source) {
        throw new UnsupportedOperationException("Client don't handle child");
    }

    @Override
    public void addHandlers(HandlersSource<D, E> source) {
        ChannelHandler handler = new ChannelInitializer<D>() {
            @Override
            public void initChannel(D ch) throws Exception {
                source.addHandlers(ch);
            }
        };
        bootstrap.handler(handler);
    }

}
