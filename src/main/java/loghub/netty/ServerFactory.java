package loghub.netty;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

public abstract class ServerFactory<D extends ServerChannel, E extends Channel> extends ComponentFactory<ServerBootstrap, ServerChannel, D, E> {

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ServerBootstrap bootstrap;

    @Override
    public AbstractBootstrap<ServerBootstrap, ServerChannel> getBootStrap() {
        bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.channelFactory(getInstance());
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    @Override
    public void group() {
        bossGroup = getEventLoopGroup();
        workerGroup = getEventLoopGroup();
        bootstrap.group(bossGroup, workerGroup);
    }

    @Override
    public void finish() {
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
    }

    @Override
    public boolean withChildHandler() {
        return true;
    }

    public void addChildhandlers(HandlersSource<D, E> source) {
        ChannelHandler handler = new ChannelInitializer<E>() {
            @Override
            public void initChannel(E ch) throws Exception {
                source.addChildHandlers(ch);
            }
        };
        bootstrap.childHandler(handler);
    }

    @Override
    public void addHandlers(HandlersSource<D, E> source) {
    }

}
